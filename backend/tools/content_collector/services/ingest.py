"""Ingest pipeline: run a fetcher, upsert items, append snapshots, update
source status.

Each fetcher round is idempotent — items are keyed by (source_id, external_id)
and only new snapshots are written on repeat sightings."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import session_factory
from ..fetchers.base import BaseFetcher, NewsItem
from ..fetchers.registry import get_fetcher
from ..models.item import Item, ItemSnapshot
from ..models.source import Source
from .score import normalize_batch

logger = logging.getLogger(__name__)


async def _get_or_create_source(session: AsyncSession, fetcher: BaseFetcher) -> Source:
    result = await session.execute(select(Source).where(Source.slug == fetcher.slug))
    source = result.scalar_one_or_none()
    if source:
        return source

    source = Source(
        slug=fetcher.slug,
        name=fetcher.name,
        lang=fetcher.lang,
        category=fetcher.category,
        region=fetcher.region,
        fetcher_type=fetcher.fetcher_type,
        fetcher_config=fetcher.fetcher_config or {},
        interval_sec=fetcher.interval_sec,
        weight=fetcher.weight,
        home_url=fetcher.home_url,
        enabled=True,
    )
    session.add(source)
    await session.flush()
    return source


async def _upsert_item(
    session: AsyncSession, source_id: int, ni: NewsItem
) -> int:
    """Upsert the item row, return its id."""
    now = datetime.now(timezone.utc)
    stmt = (
        pg_insert(Item)
        .values(
            source_id=source_id,
            external_id=str(ni.external_id),
            title=ni.title,
            url=ni.url,
            mobile_url=ni.mobile_url,
            author=ni.author,
            summary=ni.summary,
            cover=ni.cover,
            published_at=ni.published_at,
            first_seen_at=now,
            last_seen_at=now,
            extra=ni.extra or {},
        )
        .on_conflict_do_update(
            constraint="uq_items_source_external",
            set_={
                "title": ni.title,
                "url": ni.url,
                "mobile_url": ni.mobile_url,
                "summary": ni.summary,
                "last_seen_at": now,
                "extra": ni.extra or {},
            },
        )
        .returning(Item.id)
    )
    row = (await session.execute(stmt)).first()
    return int(row[0])


async def run_source(slug: str) -> dict:
    """Fetch one source and persist results. Returns a status dict."""
    fetcher = get_fetcher(slug)
    if not fetcher:
        return {"slug": slug, "status": "error", "error": "unknown fetcher"}

    started = datetime.now(timezone.utc)
    factory = session_factory()
    error: Optional[str] = None
    item_count = 0

    try:
        items = await fetcher.fetch()
    except Exception as e:  # pragma: no cover — surfaced in status
        logger.exception("fetch failed for %s", slug)
        items = []
        error = str(e)[:500]

    async with factory() as session:
        try:
            source = await _get_or_create_source(session, fetcher)

            if items and not error:
                scored = normalize_batch(items, source.weight)
                seen_item_ids: set[int] = set()
                for ni, hot_score in scored:
                    item_id = await _upsert_item(session, source.id, ni)
                    # Guard against the same external_id appearing twice in a
                    # single fetch batch — would otherwise produce duplicate
                    # snapshots (same item_id + captured_at) that make the
                    # home page render the same post twice.
                    if item_id in seen_item_ids:
                        continue
                    seen_item_ids.add(item_id)
                    session.add(
                        ItemSnapshot(
                            item_id=item_id,
                            captured_at=started,
                            hot_raw=ni.hot_raw,
                            hot_score=hot_score,
                            rank=ni.rank,
                            metrics=ni.metrics or {},
                        )
                    )
                item_count = len(seen_item_ids)

            # Update source status
            source.last_fetched_at = started
            if error:
                source.last_error = error
                source.last_error_at = started
            else:
                source.last_success_at = started
                source.last_error = None
                source.last_error_at = None

            await session.commit()
        except Exception:
            await session.rollback()
            raise

    return {
        "slug": slug,
        "status": "error" if error else "ok",
        "items": item_count,
        "error": error,
        "started_at": started.isoformat(),
    }
