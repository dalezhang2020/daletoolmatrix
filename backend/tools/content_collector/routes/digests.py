"""Digest read APIs.

GET /api/content-collector/digests          list metadata for all saved digests
GET /api/content-collector/digests/{date}   fetch one digest (YYYY-MM-DD or 'latest')
"""

from datetime import date as _date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_content_collector_db
from ..models.digest import Digest

router = APIRouter()


@router.get("")
async def list_digests(
    limit: int = Query(60, ge=1, le=365),
    db: AsyncSession = Depends(get_content_collector_db),
):
    rows = (
        await db.execute(
            select(Digest).order_by(desc(Digest.digest_date)).limit(limit)
        )
    ).scalars().all()
    return {
        "total": len(rows),
        "digests": [
            {
                "id": d.id,
                "date": d.digest_date.isoformat(),
                "generated_at": d.generated_at.isoformat(),
                "event_count": d.event_count,
                "item_count": d.item_count,
                "topic_count": d.topic_count,
                "lang_mix": d.lang_mix,
            }
            for d in rows
        ],
    }


@router.get("/{digest_date}")
async def get_digest(
    digest_date: str,
    db: AsyncSession = Depends(get_content_collector_db),
):
    if digest_date == "latest":
        digest = (
            await db.execute(
                select(Digest).order_by(desc(Digest.digest_date)).limit(1)
            )
        ).scalar_one_or_none()
    else:
        try:
            day = _date.fromisoformat(digest_date)
        except ValueError:
            raise HTTPException(400, "invalid date — use YYYY-MM-DD or 'latest'")
        digest = (
            await db.execute(select(Digest).where(Digest.digest_date == day))
        ).scalar_one_or_none()

    if not digest:
        raise HTTPException(404, "digest not found")

    return {
        "id": digest.id,
        "date": digest.digest_date.isoformat(),
        "generated_at": digest.generated_at.isoformat(),
        "window_start": digest.window_start.isoformat(),
        "window_end": digest.window_end.isoformat(),
        "summary": digest.summary,
        "event_count": digest.event_count,
        "item_count": digest.item_count,
        "topic_count": digest.topic_count,
        "lang_mix": digest.lang_mix,
        "events": digest.events_snapshot,
        "items": digest.items_snapshot,
        "topics": digest.topics_snapshot,
    }
