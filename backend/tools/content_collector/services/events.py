"""Event detection — promote qualifying topics to deduplicated Event rows.

A topic becomes an Event when all of:
  - at least MIN_SOURCE_DIVERSITY distinct sources cover it
  - cumulative hot_score * weight >= MIN_TOTAL_SCORE
  - activity happened within the last MAX_WINDOW_HOURS

Identity: each event has a stable `signature` derived from its top keyword +
lang, so detect_events() is idempotent — running it N times produces the same
rows (updated, not duplicated). Member items are snapshotted into event_items
so events survive topic re-clustering.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import session_factory
from ..models.event import Event, EventItem
from ..models.item import Item, ItemSnapshot
from ..models.source import Source
from ..models.topic import Topic, TopicItem

logger = logging.getLogger(__name__)

# Tunables — constants for now, elevate to config if product needs it.
# Thresholds tightened: we only surface events Dale actually wants to see.
MIN_SOURCE_DIVERSITY = 3
MIN_ITEM_COUNT = 5
MIN_TOTAL_SCORE = 500.0
MAX_WINDOW_HOURS = 24

# Only topics whose majority items are in these categories become events.
# Entertainment/sports/gaming/social-drama topics reliably hit the multi-
# source threshold but are explicitly not what Dale tracks.
ALLOWED_EVENT_CATEGORIES = {"news", "knowledge"}
# Of those allowed, require at least this fraction of items to agree on
# category. Stops a mostly-entertainment topic from getting classified as
# an event just because 1 of 5 items happens to be tagged 'news'.
MIN_CATEGORY_MAJORITY = 0.6


def _signature(lang: str, keywords: list[str]) -> str:
    """Stable fingerprint. We use lang + top keyword so minor re-clustering
    noise (e.g. keyword #2 vs #3 swapping) doesn't break dedup."""
    top = (keywords[0] if keywords else "").strip().lower()
    h = hashlib.sha1(f"{lang}|{top}".encode("utf-8")).hexdigest()[:24]
    return f"{lang}:{h}"


async def _topic_stats(
    session: AsyncSession, topic_id: int, since: datetime
) -> tuple[int, float, list[int], dict[str, int]]:
    """Return (distinct_source_count, cumulative_score, member_item_ids,
    category_counts)."""
    # Gather member item ids
    item_ids = (
        await session.execute(
            select(TopicItem.item_id).where(TopicItem.topic_id == topic_id)
        )
    ).scalars().all()
    if not item_ids:
        return 0, 0.0, [], {}

    stats = (
        await session.execute(
            select(
                func.count(func.distinct(Item.source_id)),
                func.coalesce(
                    func.sum(ItemSnapshot.hot_score * Source.weight), 0.0
                ),
            )
            .select_from(Item)
            .join(Source, Source.id == Item.source_id)
            .join(ItemSnapshot, ItemSnapshot.item_id == Item.id)
            .where(Item.id.in_(list(item_ids)))
            .where(Source.enabled.is_(True))
            .where(Item.first_seen_at >= since)
        )
    ).first()
    diversity = int(stats[0] or 0)
    score = float(stats[1] or 0.0)

    # Category distribution within the topic — used to filter out events
    # that don't belong in Dale's interest categories.
    cat_rows = (
        await session.execute(
            select(Item.category, func.count(Item.id))
            .where(Item.id.in_(list(item_ids)))
            .group_by(Item.category)
        )
    ).all()
    category_counts: dict[str, int] = {
        (c or "_unknown"): int(n) for c, n in cat_rows
    }

    return diversity, score, [int(i) for i in item_ids], category_counts


async def detect_events() -> dict:
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=MAX_WINDOW_HOURS)

    factory = session_factory()
    created = 0
    updated = 0

    async with factory() as session:
        try:
            topics = (
                await session.execute(
                    select(Topic).where(Topic.last_item_at >= since)
                )
            ).scalars().all()

            for topic in topics:
                diversity, score, item_ids, category_counts = await _topic_stats(
                    session, topic.id, since
                )
                if (
                    diversity < MIN_SOURCE_DIVERSITY
                    or score < MIN_TOTAL_SCORE
                    or len(item_ids) < MIN_ITEM_COUNT
                ):
                    continue

                # Category gate: the majority of items must be in one of
                # Dale's tracked categories. Drops entertainment / sports /
                # gaming / gossip topics before they hit the Events tab.
                total_items = sum(category_counts.values()) or 1
                allowed_items = sum(
                    n for c, n in category_counts.items()
                    if c in ALLOWED_EVENT_CATEGORIES
                )
                if allowed_items / total_items < MIN_CATEGORY_MAJORITY:
                    continue

                sig = _signature(topic.lang, topic.keywords or [topic.label])
                summary = topic.label
                if topic.keywords and len(topic.keywords) > 1:
                    summary = f"{topic.label} · " + ", ".join(topic.keywords[1:4])

                # Upsert event by signature
                stmt = (
                    pg_insert(Event)
                    .values(
                        signature=sig,
                        label=topic.label,
                        keywords=list(topic.keywords or []),
                        lang=topic.lang,
                        summary=summary,
                        first_detected_at=now,
                        last_seen_at=now,
                        source_count=diversity,
                        peak_score=score,
                        item_count=len(item_ids),
                        active=True,
                    )
                    .on_conflict_do_update(
                        index_elements=["signature"],
                        set_={
                            "label": topic.label,
                            "keywords": list(topic.keywords or []),
                            "summary": summary,
                            "last_seen_at": now,
                            "source_count": diversity,
                            "peak_score": func.greatest(Event.peak_score, score),
                            "item_count": len(item_ids),
                            "active": True,
                        },
                    )
                    .returning(Event.id, Event.first_detected_at)
                )
                row = (await session.execute(stmt)).first()
                event_id = int(row[0])
                # If first_detected_at equals `now` (within the same tx), it's a new row.
                if row[1] == now:
                    created += 1
                else:
                    updated += 1

                # Snapshot members — insert new links idempotently.
                for iid in item_ids:
                    await session.execute(
                        pg_insert(EventItem)
                        .values(event_id=event_id, item_id=iid)
                        .on_conflict_do_nothing(
                            index_elements=["event_id", "item_id"]
                        )
                    )

            # Mark events not touched in this run as inactive (they've aged out
            # of the 24h window). Leave them in DB for history.
            await session.execute(
                Event.__table__.update()
                .where(Event.last_seen_at < since)
                .values(active=False)
            )

            await session.commit()
        except Exception:
            await session.rollback()
            raise

    logger.info(
        "content_collector: events done (created=%d updated=%d)",
        created,
        updated,
    )
    return {"created": created, "updated": updated}
