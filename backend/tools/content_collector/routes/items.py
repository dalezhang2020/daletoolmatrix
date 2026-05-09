"""GET /api/content-collector/items — the 7-day hot list."""

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_content_collector_db
from ..models.item import Item, ItemSnapshot
from ..models.source import Source

router = APIRouter()

# Half-life for the time-decay ranking, in hours. An item halves in rank
# influence every 12 hours, so 48h-old posts sit at 1/16 of their peak score.
_HALFLIFE_HOURS = 12.0
# Lower bound on decay so extremely old items can still appear when nothing
# fresher exists, instead of rounding to zero.
_MIN_DECAY = 0.02


@router.get("")
async def list_items(
    lang: Optional[str] = Query(None, description="zh or en"),
    source: Optional[str] = Query(
        None,
        description=(
            "Source slug, or a comma-separated list of slugs to match any "
            "(used by the frontend source-group pills, e.g. "
            "'bbc_world,nyt_top' for the 'World News' group)."
        ),
    ),
    category: Optional[str] = Query(None, description="category slug"),
    exclude_category: Optional[str] = Query(
        None,
        description="comma-separated category slugs to exclude (e.g. 'other')",
    ),
    days: int = Query(7, ge=1, le=30),
    limit: int = Query(100, ge=1, le=500),
    sort: str = Query(
        "ranked",
        pattern="^(ranked|latest|hot)$",
        description=(
            "ranked = hot_score * source.weight * time-decay (default); "
            "latest = first_seen_at desc; "
            "hot = hot_score desc (ignores source weight and recency)"
        ),
    ),
    db: AsyncSession = Depends(get_content_collector_db),
):
    """Top items in the last N days.

    Dedup strategy: one row per Item, using each item's single most recent
    snapshot. Duplicates in ItemSnapshot (same item_id + captured_at) used to
    cause the same post to appear twice in the UI — this is now prevented by
    joining through a GROUP BY subquery and tie-breaking on MAX(snapshot.id).
    """
    since = datetime.now(timezone.utc) - timedelta(days=days)

    # One latest snapshot per item within the window.
    # MAX(id) as the tiebreaker so even if two snapshots share captured_at
    # we still pick exactly one row per item.
    latest = (
        select(
            ItemSnapshot.item_id.label("item_id"),
            func.max(ItemSnapshot.captured_at).label("captured_at"),
            func.max(ItemSnapshot.id).label("snap_id"),
        )
        .where(ItemSnapshot.captured_at >= since)
        .group_by(ItemSnapshot.item_id)
        .subquery()
    )

    # Time-decay factor: 2 ** (-age_hours / halflife), clamped to _MIN_DECAY.
    # Age is measured from first_seen_at (when WE first saw the post); that's
    # more honest for ranking than published_at which many sources lie about.
    now_utc = datetime.now(timezone.utc)
    age_hours = func.extract(
        "epoch", now_utc - Item.first_seen_at
    ) / 3600.0
    # Postgres-friendly expression: GREATEST(MIN_DECAY, POWER(0.5, age_h / H))
    decay = func.greatest(
        _MIN_DECAY,
        func.power(0.5, age_hours / _HALFLIFE_HOURS),
    )
    ranked_expr = (ItemSnapshot.hot_score * Source.weight * decay).label(
        "ranked_score"
    )

    q = (
        select(
            Item.id,
            Item.title,
            Item.url,
            Item.mobile_url,
            Item.author,
            Item.summary,
            Item.title_zh,
            Item.summary_zh,
            Item.published_at,
            Item.first_seen_at,
            Item.category,
            Source.slug.label("source_slug"),
            Source.name.label("source_name"),
            Source.lang.label("source_lang"),
            Source.weight.label("source_weight"),
            ItemSnapshot.hot_score,
            ItemSnapshot.hot_raw,
            ItemSnapshot.rank,
            ItemSnapshot.captured_at,
            ranked_expr,
        )
        .join(Source, Source.id == Item.source_id)
        .join(latest, latest.c.item_id == Item.id)
        .join(
            ItemSnapshot,
            ItemSnapshot.id == latest.c.snap_id,
        )
        .where(Item.first_seen_at >= since)
        .where(Source.enabled.is_(True))
    )

    if lang:
        q = q.where(Source.lang == lang)
    if source:
        slugs = [s.strip() for s in source.split(",") if s.strip()]
        if len(slugs) == 1:
            q = q.where(Source.slug == slugs[0])
        elif slugs:
            q = q.where(Source.slug.in_(slugs))
    if category:
        q = q.where(Item.category == category)
    if exclude_category:
        excluded = [c.strip() for c in exclude_category.split(",") if c.strip()]
        if excluded:
            q = q.where(Item.category.notin_(excluded))

    # Order + limit applied last so sort switching doesn't leak double limits.
    if sort == "latest":
        q = q.order_by(Item.first_seen_at.desc())
    elif sort == "hot":
        q = q.order_by(ItemSnapshot.hot_score.desc())
    else:  # "ranked"
        q = q.order_by(ranked_expr.desc())
    q = q.limit(limit)

    rows = (await db.execute(q)).mappings().all()

    return {
        "total": len(rows),
        "window_days": days,
        "items": [
            {
                "id": r["id"],
                "title": r["title"],
                "title_zh": r["title_zh"],
                "url": r["url"],
                "mobile_url": r["mobile_url"],
                "author": r["author"],
                "summary": r["summary"],
                "summary_zh": r["summary_zh"],
                "source": {
                    "slug": r["source_slug"],
                    "name": r["source_name"],
                    "lang": r["source_lang"],
                },
                "category": r["category"],
                "hot_score": r["hot_score"],
                "hot_raw": r["hot_raw"],
                "rank": r["rank"],
                "ranked_score": float(r["ranked_score"]) if r["ranked_score"] is not None else None,
                "published_at": r["published_at"].isoformat() if r["published_at"] else None,
                "first_seen_at": r["first_seen_at"].isoformat() if r["first_seen_at"] else None,
                "captured_at": r["captured_at"].isoformat() if r["captured_at"] else None,
            }
            for r in rows
        ],
    }



@router.get("/categories")
async def list_categories(
    days: int = Query(7, ge=1, le=30),
    lang: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_content_collector_db),
):
    """Return all category definitions plus item counts in the last N days.

    Consumed by the frontend to render a category tab bar with counts.
    """
    from sqlalchemy import func as _f

    from ..services.categorize import CATEGORIES

    since = datetime.now(timezone.utc) - timedelta(days=days)

    counts_q = (
        select(Item.category, _f.count(Item.id).label("n"))
        .join(Source, Source.id == Item.source_id)
        .where(Item.first_seen_at >= since)
        .where(Source.enabled.is_(True))
        .group_by(Item.category)
    )
    if lang:
        counts_q = counts_q.where(Source.lang == lang)

    rows = (await db.execute(counts_q)).all()
    counts: dict[str, int] = {r[0] or "_pending": int(r[1]) for r in rows}

    return {
        "window_days": days,
        "total": sum(counts.values()),
        "pending": counts.get("_pending", 0),
        "categories": [
            {
                "slug": slug,
                "name_zh": meta["zh"],
                "name_en": meta["en"],
                "description": meta["desc"],
                "count": counts.get(slug, 0),
            }
            for slug, meta in CATEGORIES.items()
        ],
    }
