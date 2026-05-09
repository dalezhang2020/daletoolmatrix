"""Daily digest — snapshots the day into the digests table.

Job runs at 08:00 America/New_York. It captures:
  - top 10 active events (label, keywords, source_count, peak_score, sample items)
  - top 20 hot items from the last 24h
  - top 10 topics by total_score

Data is copied, not referenced — digests stay readable forever even after
retention GC removes the underlying items/events.

Identity: `digest_date` (calendar day in America/New_York) is UNIQUE, so
re-running the same day updates the existing row. Any day can be regenerated
via POST /admin/generate-digest?date=YYYY-MM-DD.
"""

from __future__ import annotations

import json
import logging
import os
from collections import Counter
from datetime import date as _date, datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import httpx

from sqlalchemy import desc, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..database import session_factory
from ..models.digest import Digest
from ..models.event import Event, EventItem
from ..models.item import Item, ItemSnapshot
from ..models.source import Source

logger = logging.getLogger(__name__)

TZ = ZoneInfo("America/New_York")


_SUMMARY_SYSTEM_PROMPT = (
    "You are writing a daily AI & tech briefing in Chinese for a single reader "
    "who works in tech and wants to stay current on AI, software, and the startup "
    "ecosystem. The briefing covers yesterday's most-discussed items.\n\n"
    "RULES:\n"
    "1. Write in fluent, natural Simplified Chinese. Keep proper nouns (product "
    "names, company names, people, technical terms, version numbers) in their "
    "original English — do NOT transliterate them.\n"
    "2. DO NOT compress or over-summarize. Each item that is genuinely interesting "
    "deserves a full sentence or two. If an item has a meaningful summary, use it.\n"
    "3. Group related items naturally if they share a theme (e.g. multiple Claude "
    "updates, several LLM benchmark papers). Otherwise list them individually.\n"
    "4. Tone: concise but substantive. Like a smart colleague telling you what "
    "happened yesterday — not a press release, not a tweet thread.\n"
    "5. Length: aim for 400-800 Chinese characters. Do not pad; do not truncate "
    "interesting content to hit a word count.\n"
    "6. Start directly with the content — no greeting, no 'Today's briefing:', "
    "no meta-commentary about what you're about to do.\n"
    "7. End with a blank line, then a one-sentence '今日一句' — a single sharp "
    "observation or question the day's news raises, in Chinese.\n"
)


async def _generate_summary(items: list[dict]) -> str | None:
    """Call the LLM to produce a Chinese briefing from the day's knowledge items.

    `items` is a list of dicts with keys: title, title_zh, summary, summary_zh,
    source_name. Returns None on any failure so the digest is still saved
    without a summary rather than failing entirely.
    """
    if not items:
        return None

    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    model = os.getenv("AZURE_OPENAI_CHAT_MODEL", "gpt-4o-mini")
    if not (endpoint and api_key):
        logger.warning("digest: LLM summary skipped (missing AZURE_OPENAI_*)")
        return None

    # Build the item list for the prompt. Use translated title/summary when
    # available; fall back to English originals.
    lines: list[str] = []
    for i, it in enumerate(items, 1):
        title = it.get("title_zh") or it.get("title") or ""
        summary = it.get("summary_zh") or it.get("summary") or ""
        source = it.get("source_name") or ""
        line = f"{i}. [{source}] {title}"
        if summary and len(summary) > 20:
            # Cap summary at 500 chars to keep prompt size bounded
            line += f"\n   摘要：{summary[:500]}"
        lines.append(line)

    user_payload = "\n".join(lines)

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                endpoint.rstrip("/") + "/chat/completions",
                headers={"api-key": api_key, "Content-Type": "application/json"},
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": _SUMMARY_SYSTEM_PROMPT},
                        {"role": "user", "content": user_payload},
                    ],
                    "temperature": 0.4,
                    "max_completion_tokens": 1500,
                },
            )
            if resp.status_code >= 400:
                logger.warning(
                    "digest: LLM summary HTTP %d — %s",
                    resp.status_code,
                    resp.text[:300],
                )
                return None
            return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.warning("digest: LLM summary failed: %s", e)
        return None


def _window_for_day(d: _date) -> tuple[datetime, datetime]:
    """Return (start_utc, end_utc) for a calendar day in America/New_York."""
    start_local = datetime(d.year, d.month, d.day, tzinfo=TZ)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


async def generate_digest(
    target_date: Optional[_date] = None,
    with_summary: bool = False,
) -> dict:
    """Generate/regenerate the digest for the given calendar day.

    Default is YESTERDAY in America/New_York. Rationale: this function is
    invoked by the 08:00 ET cron each morning — at that time "yesterday" is
    a completed 24-hour day, which is exactly what Dale wants to read over
    breakfast. Explicit dates always win.

    `with_summary=True` triggers an LLM call to produce a Chinese briefing
    of the day's knowledge/tech items. Only set this for the morning cron;
    rolling today-digests skip it to save tokens.

    Window for a date D = D 00:00 ET → (D+1) 00:00 ET.
    """
    if target_date is None:
        target_date = (datetime.now(TZ) - timedelta(days=1)).date()

    start_utc, end_utc = _window_for_day(target_date)
    factory = session_factory()

    async with factory() as session:
        try:
            # ---------- events ----------
            events = (
                await session.execute(
                    select(Event)
                    .where(Event.last_seen_at >= start_utc)
                    .where(Event.last_seen_at < end_utc + timedelta(hours=12))
                    .order_by(desc(Event.peak_score))
                    .limit(10)
                )
            ).scalars().all()

            # If nothing fell into that tighter window (common early on), fall back
            # to "currently active events" so the digest isn't empty.
            if not events:
                events = (
                    await session.execute(
                        select(Event)
                        .where(Event.active.is_(True))
                        .order_by(desc(Event.peak_score))
                        .limit(10)
                    )
                ).scalars().all()

            events_snapshot = []
            for e in events:
                # Sample items for this event (top 5)
                sample_rows = (
                    await session.execute(
                        select(Item.title, Item.url, Source.name.label("source_name"))
                        .join(Source, Source.id == Item.source_id)
                        .join(EventItem, EventItem.item_id == Item.id)
                        .where(EventItem.event_id == e.id)
                        .order_by(Item.first_seen_at.desc())
                        .limit(5)
                    )
                ).mappings().all()
                events_snapshot.append(
                    {
                        "id": e.id,
                        "label": e.label,
                        "keywords": list(e.keywords or []),
                        "lang": e.lang,
                        "summary": e.summary,
                        "source_count": e.source_count,
                        "item_count": e.item_count,
                        "peak_score": e.peak_score,
                        "sample_items": [
                            {
                                "title": r["title"],
                                "url": r["url"],
                                "source_name": r["source_name"],
                            }
                            for r in sample_rows
                        ],
                    }
                )

            # ---------- hottest items in the window ----------
            latest = (
                select(
                    ItemSnapshot.item_id.label("item_id"),
                    func.max(ItemSnapshot.captured_at).label("captured_at"),
                )
                .where(ItemSnapshot.captured_at >= start_utc)
                .where(ItemSnapshot.captured_at < end_utc)
                .group_by(ItemSnapshot.item_id)
                .subquery()
            )

            item_rows = (
                await session.execute(
                    select(
                        Item.id,
                        Item.title,
                        Item.title_zh,
                        Item.url,
                        Source.slug.label("source_slug"),
                        Source.name.label("source_name"),
                        Source.lang.label("source_lang"),
                        ItemSnapshot.hot_score,
                        ItemSnapshot.hot_raw,
                    )
                    .join(Source, Source.id == Item.source_id)
                    .join(latest, latest.c.item_id == Item.id)
                    .join(
                        ItemSnapshot,
                        (ItemSnapshot.item_id == latest.c.item_id)
                        & (ItemSnapshot.captured_at == latest.c.captured_at),
                    )
                    .order_by(desc(ItemSnapshot.hot_score * Source.weight))
                    .limit(20)
                )
            ).mappings().all()

            items_snapshot = [
                {
                    "id": r["id"],
                    "title": r["title"],
                    "title_zh": r["title_zh"],
                    "url": r["url"],
                    "source_slug": r["source_slug"],
                    "source_name": r["source_name"],
                    "source_lang": r["source_lang"],
                    "hot_score": r["hot_score"],
                    "hot_raw": r["hot_raw"],
                }
                for r in item_rows
            ]

            # ---------- AI summary (knowledge items only) ----------
            # Only generated when with_summary=True (morning cron).
            # Query directly from items table (not snapshot window) so we
            # always have content even if snapshots are sparse.
            digest_summary: str | None = None
            if with_summary:
                summary_rows = (
                    await session.execute(
                        select(
                            Item.id,
                            Item.title,
                            Item.title_zh,
                            Item.summary,
                            Item.summary_zh,
                            Source.name.label("source_name"),
                        )
                        .join(Source, Source.id == Item.source_id)
                        .where(Item.first_seen_at >= start_utc)
                        .where(Item.first_seen_at < end_utc)
                        .where(Item.category == "knowledge")
                        .where(Source.lang == "en")
                        .where(Source.enabled.is_(True))
                        .order_by(Item.first_seen_at.desc())
                        .limit(30)
                    )
                ).mappings().all()
                if summary_rows:
                    digest_summary = await _generate_summary(
                        [dict(r) for r in summary_rows]
                    )
                else:
                    logger.info(
                        "digest: no knowledge items in window %s–%s, skipping summary",
                        start_utc.date(),
                        end_utc.date(),
                    )

            # ---------- top topics ----------
            from ..models.topic import Topic

            topics = (
                await session.execute(
                    select(Topic)
                    .where(Topic.last_item_at >= start_utc)
                    .order_by(desc(Topic.total_score * Topic.source_diversity))
                    .limit(10)
                )
            ).scalars().all()

            topics_snapshot = [
                {
                    "id": t.id,
                    "label": t.label,
                    "keywords": list(t.keywords or []),
                    "lang": t.lang,
                    "item_count": t.item_count,
                    "source_diversity": t.source_diversity,
                    "total_score": t.total_score,
                }
                for t in topics
            ]

            # ---------- language mix ----------
            lang_counter = Counter(it["source_lang"] for it in items_snapshot)

            # ---------- upsert digest ----------
            now = datetime.now(timezone.utc)
            update_fields: dict = {
                "generated_at": now,
                "events_snapshot": events_snapshot,
                "items_snapshot": items_snapshot,
                "topics_snapshot": topics_snapshot,
                "event_count": len(events_snapshot),
                "item_count": len(items_snapshot),
                "topic_count": len(topics_snapshot),
                "lang_mix": dict(lang_counter),
            }
            if with_summary:
                update_fields["summary"] = digest_summary

            stmt = (
                pg_insert(Digest)
                .values(
                    digest_date=target_date,
                    generated_at=now,
                    window_start=start_utc,
                    window_end=end_utc,
                    summary=digest_summary,
                    events_snapshot=events_snapshot,
                    items_snapshot=items_snapshot,
                    topics_snapshot=topics_snapshot,
                    event_count=len(events_snapshot),
                    item_count=len(items_snapshot),
                    topic_count=len(topics_snapshot),
                    lang_mix=dict(lang_counter),
                )
                .on_conflict_do_update(
                    index_elements=["digest_date"],
                    set_=update_fields,
                )
                .returning(Digest.id)
            )
            row = (await session.execute(stmt)).first()
            digest_id = int(row[0])

            await session.commit()

        except Exception:
            await session.rollback()
            raise

    logger.info(
        "content_collector: digest saved (date=%s events=%d items=%d topics=%d)",
        target_date,
        len(events_snapshot),
        len(items_snapshot),
        len(topics_snapshot),
    )
    return {
        "id": digest_id,
        "date": target_date.isoformat(),
        "events": len(events_snapshot),
        "items": len(items_snapshot),
        "topics": len(topics_snapshot),
    }
