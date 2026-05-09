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


@router.get("")
async def list_items(
    lang: Optional[str] = Query(None, description="zh or en"),
    source: Optional[str] = Query(None, description="source slug"),
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
        description="ranked = hot_score * source.weight; latest = first_seen_at desc; hot = hot_score desc",
    ),
    db: AsyncSession = Depends(get_content_collector_db),
):
    """Top items in the last N days, ranked by their most recent hot_score."""
    since = datetime.now(timezone.utc) - timedelta(days=days)

    # Latest snapshot per item within the window
    latest = (
        select(
            ItemSnapshot.item_id.label("item_id"),
            func.max(ItemSnapshot.captured_at).label("captured_at"),
        )
        .where(ItemSnapshot.captured_at >= since)
        .group_by(ItemSnapshot.item_id)
        .subquery()
    )

    q = (
        select(
            Item.id,
            Item.title,
            Item.url,
            Item.mobile_url,
            Item.author,
            Item.summary,
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
            (ItemSnapshot.hot_score * Source.weight).label("ranked_score"),
        )
        .join(Source, Source.id == Item.source_id)
        .join(latest, latest.c.item_id == Item.id)
        .join(
            ItemSnapshot,
            (ItemSnapshot.item_id == latest.c.item_id)
            & (ItemSnapshot.captured_at == latest.c.captured_at),
        )
        .where(Item.first_seen_at >= since)
        .order_by((ItemSnapshot.hot_score * Source.weight).desc())
        .limit(limit)
    )

    if lang:
        q = q.where(Source.lang == lang)
    if source:
        q = q.where(Source.slug == source)
    if category:
        q = q.where(Item.category == category)
    if exclude_category:
        excluded = [c.strip() for c in exclude_category.split(",") if c.strip()]
        if excluded:
            q = q.where(Item.category.notin_(excluded))

    # Swap the ORDER BY per requested sort mode
    if sort == "latest":
        q = q.order_by(None).order_by(Item.first_seen_at.desc()).limit(limit)
    elif sort == "hot":
        q = q.order_by(None).order_by(ItemSnapshot.hot_score.desc()).limit(limit)
    # else: "ranked" is already the default ORDER BY

    rows = (await db.execute(q)).mappings().all()

    return {
        "total": len(rows),
        "window_days": days,
        "items": [
            {
                "id": r["id"],
                "title": r["title"],
                "url": r["url"],
                "mobile_url": r["mobile_url"],
                "author": r["author"],
                "summary": r["summary"],
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
