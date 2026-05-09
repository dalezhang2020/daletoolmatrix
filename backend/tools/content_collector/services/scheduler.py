"""APScheduler-backed background runner.

One job per fetcher, each running on its own interval_sec. Startup also runs
every fetcher once so the dashboard has data immediately. Safe to disable via
CONTENT_COLLECTOR_SCHEDULER_ENABLED=false.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from ..config import content_collector_settings
from ..fetchers.registry import get_registry
from .cluster import recluster_all
from .categorize import categorize_backlog
from .digest import generate_digest
from .events import detect_events
from .ingest import run_source
from .translate import translate_backlog

logger = logging.getLogger(__name__)

_scheduler: Optional[AsyncIOScheduler] = None


def _start_initial_runs() -> None:
    """Kick off an immediate fetch for every fetcher, in the background."""

    async def _runner():
        slugs = list(get_registry().keys())
        logger.info("content_collector: initial fetch for %d sources", len(slugs))
        # Stagger a bit so we don't hammer all sources simultaneously
        for i, slug in enumerate(slugs):
            await asyncio.sleep(0.3 * i)
            try:
                result = await run_source(slug)
                logger.info("initial fetch %s -> %s", slug, result.get("status"))
            except Exception:
                logger.exception("initial fetch failed for %s", slug)

        # Once we have fresh items, do an initial cluster + event + category pass
        try:
            await recluster_all()
            await detect_events()
            # Initial classification pass on startup: rules + source fallback
            # ONLY. LLM is intentionally off here — don't pay to classify a
            # fresh boot; the scheduled 6h LLM pass will mop up what rules
            # missed. Run enough batches to drain whatever rules can handle.
            for _ in range(20):
                r = await categorize_backlog(use_llm=False)
                if r.get("processed", 0) == 0:
                    break
        except Exception:
            logger.exception("initial cluster/events/categorize failed")

        # And an initial digest for today so the UI has something to show.
        try:
            from datetime import datetime as _dt
            from zoneinfo import ZoneInfo as _TZ

            today_et = _dt.now(_TZ("America/New_York")).date()
            await generate_digest(today_et)
        except Exception:
            logger.exception("initial digest failed")

    asyncio.create_task(_runner())


def start_scheduler() -> None:
    global _scheduler
    if not content_collector_settings.scheduler_enabled:
        logger.info("content_collector: scheduler disabled via env")
        return
    if _scheduler is not None:
        return

    _scheduler = AsyncIOScheduler()
    registry = get_registry()

    # Unify all source fetchers to a 2-hour cadence. Dale's UI auto-refreshes
    # every 2h — fetching faster than the UI can show new data just burns
    # rate limits on source sites and API tokens downstream.
    FETCH_INTERVAL_SEC = 2 * 60 * 60

    for slug, fetcher in registry.items():
        _scheduler.add_job(
            run_source,
            trigger=IntervalTrigger(seconds=FETCH_INTERVAL_SEC),
            args=[slug],
            id=f"content_collector:{slug}",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
    logger.info(
        "content_collector: scheduled %d sources every %ds",
        len(registry),
        FETCH_INTERVAL_SEC,
    )

    # Topic clustering every 2 hours — aligned with fetch cadence.
    _scheduler.add_job(
        recluster_all,
        trigger=IntervalTrigger(hours=2),
        id="content_collector:cluster",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    # Event detection right after fetch — 2h aligned
    _scheduler.add_job(
        detect_events,
        trigger=IntervalTrigger(hours=2),
        id="content_collector:events",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # Categorize — cheap pass (rules + source fallback only) every 30min so
    # new items get a label quickly for free.
    async def _categorize_rules_only():
        return await categorize_backlog(use_llm=False)

    _scheduler.add_job(
        _categorize_rules_only,
        trigger=IntervalTrigger(minutes=30),
        id="content_collector:categorize_rules",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # LLM pass — runs every 6 hours, capped at 100 items per batch so we
    # can never spend more than ~$0.02 per call even if the backlog is huge.
    # Anything above the cap waits for the next tick.
    async def _categorize_with_llm():
        return await categorize_backlog(use_llm=True, llm_cap=100)

    _scheduler.add_job(
        _categorize_with_llm,
        trigger=IntervalTrigger(hours=6),
        id="content_collector:categorize_llm",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # Translation — same 6h cadence as the LLM classifier, capped at 30
    # items per call (roughly $0.001 per run). Only touches EN-source
    # items in knowledge/news categories.
    async def _translate():
        return await translate_backlog(llm_cap=30)

    _scheduler.add_job(
        _translate,
        trigger=IntervalTrigger(hours=6),
        id="content_collector:translate",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    logger.info(
        "content_collector: scheduled cluster+events (2h), "
        "categorize rules (30m), categorize LLM (6h, cap=100), "
        "translate (6h, cap=30)"
    )

    # Daily digest snapshot — 08:00 Eastern (Dale's timezone).
    # Captures YESTERDAY's completed day for morning reading.
    _scheduler.add_job(
        generate_digest,
        trigger=CronTrigger(hour=8, minute=0, timezone="America/New_York"),
        id="content_collector:digest_yesterday",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # Rolling "today" digest — regenerates every 2h so the UI always has
    # fresh-ish today-so-far data to display alongside yesterday's final.
    async def _digest_today():
        from datetime import datetime as _dt
        from zoneinfo import ZoneInfo as _TZ

        today_et = _dt.now(_TZ("America/New_York")).date()
        return await generate_digest(today_et)

    _scheduler.add_job(
        _digest_today,
        trigger=IntervalTrigger(hours=2),
        id="content_collector:digest_today",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    logger.info(
        "content_collector: scheduled digest (yesterday @ 08:00 ET, today every 2h)"
    )

    _scheduler.start()
    _start_initial_runs()


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler is None:
        return
    _scheduler.shutdown(wait=False)
    _scheduler = None
