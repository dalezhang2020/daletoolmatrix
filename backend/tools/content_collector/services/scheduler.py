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
            # Run categorize a few times to drain the initial backlog (each call
            # handles 50; we usually have 300-500 items on first boot).
            for _ in range(20):
                r = await categorize_backlog()
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

    # Unify all source fetchers to a 1-hour cadence. Per-source intervals
    # (e.g. 30min, 2h) were useful earlier but add schedule noise and never
    # translate into more useful data — the UI refreshes every 2h anyway.
    FETCH_INTERVAL_SEC = 60 * 60

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

    # Topic clustering every hour — aligned with fetch cadence so clusters
    # reflect each new batch without rerunning uselessly between fetches.
    _scheduler.add_job(
        recluster_all,
        trigger=IntervalTrigger(hours=1),
        id="content_collector:cluster",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    # Event detection right after fetch cycle — 1h aligned
    _scheduler.add_job(
        detect_events,
        trigger=IntervalTrigger(hours=1),
        id="content_collector:events",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    # Categorize backlog every 10 min — fetchers arrive hourly, but we still
    # want freshly-arrived items classified within 10 min of landing so the
    # dashboard filters are never empty.
    _scheduler.add_job(
        categorize_backlog,
        trigger=IntervalTrigger(minutes=10),
        id="content_collector:categorize",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    logger.info("content_collector: scheduled cluster + events (1h), categorize (10m)")

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
