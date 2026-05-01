"""Periodic job to refresh expiring Shopline access tokens.

Uses a simple ``asyncio`` background task (no APScheduler dependency).
Call :func:`start_refresh_job` once at application startup to begin the
periodic loop, and :func:`stop_refresh_job` at shutdown to cancel it.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from backend.tools.shopline_zendesk.db import store_repo
from backend.tools.shopline_zendesk.services.shopline_auth import refresh_token

logger = logging.getLogger(__name__)

# Default interval between refresh sweeps (in minutes).
DEFAULT_INTERVAL_MINUTES = 60

# Number of consecutive failures before a token is marked invalid.
_MAX_FAIL_COUNT = 3


class TokenRefreshJob:
    """Refresh expiring Shopline access tokens on a configurable schedule.

    Parameters
    ----------
    interval_minutes:
        How often (in minutes) the job queries for expiring tokens.
        Defaults to ``DEFAULT_INTERVAL_MINUTES`` (60).
    """

    def __init__(self, interval_minutes: int = DEFAULT_INTERVAL_MINUTES) -> None:
        self.interval_minutes = interval_minutes
        self._task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self) -> dict:
        """Query expiring tokens and refresh each one.

        Returns a summary dict with counts of successes and failures.
        """
        logger.info("Token refresh job started")

        expiring_stores = store_repo.get_expiring_stores(hours=2)
        logger.info("Found %d store(s) with expiring tokens", len(expiring_stores))

        success_count = 0
        fail_count = 0

        for store in expiring_stores:
            handle: str = store["handle"]
            old_token: str = store["access_token"]
            try:
                ok = await self.refresh_single_store(handle, old_token)
                if ok:
                    success_count += 1
                else:
                    fail_count += 1
            except Exception:
                # Defensive catch-all — should not happen because
                # refresh_single_store already has its own try/except,
                # but we never let one store break the loop.
                logger.exception(
                    "Unexpected error refreshing: handle=%s zendesk_subdomain=%s "
                    "error_type=token_refresh_error timestamp=%s",
                    handle,
                    "",
                    datetime.now(timezone.utc).isoformat(),
                )
                fail_count += 1

        summary = {
            "total": len(expiring_stores),
            "success": success_count,
            "failed": fail_count,
        }
        logger.info("Token refresh job finished: %s", summary)
        return summary

    async def refresh_single_store(self, handle: str, old_token: str) -> bool:
        """Refresh a single store's token.

        On success: updates the token in the DB and resets the fail count.
        On failure: increments ``refresh_fail_count`` and marks the token
        invalid when the count reaches ``_MAX_FAIL_COUNT``.

        Returns ``True`` on success, ``False`` on failure.
        """
        try:
            new_token, expires_at = await refresh_token(handle, old_token)

            store_repo.update_token(handle, new_token, expires_at)
            store_repo.reset_refresh_fail_count(handle)

            logger.info(
                "Token refreshed successfully for handle=%s, new_expires_at=%s",
                handle,
                expires_at.isoformat(),
            )
            return True

        except Exception:
            logger.exception(
                "Token refresh failed: handle=%s zendesk_subdomain=%s "
                "error_type=token_refresh_error timestamp=%s",
                handle,
                "",  # zendesk_subdomain not available in refresh context
                datetime.now(timezone.utc).isoformat(),
            )

            updated = store_repo.increment_refresh_fail_count(handle)

            if updated and updated.get("refresh_fail_count", 0) >= _MAX_FAIL_COUNT:
                store_repo.mark_token_invalid(handle)
                logger.warning(
                    "Token marked invalid after %d consecutive failures: "
                    "handle=%s zendesk_subdomain=%s "
                    "error_type=token_refresh_error timestamp=%s",
                    _MAX_FAIL_COUNT,
                    handle,
                    "",  # zendesk_subdomain not available in refresh context
                    datetime.now(timezone.utc).isoformat(),
                )

            return False

    # ------------------------------------------------------------------
    # Background loop management
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the periodic refresh loop as an ``asyncio`` background task."""
        if self._task is not None and not self._task.done():
            logger.warning("Token refresh job is already running")
            return

        self._task = asyncio.get_event_loop().create_task(self._loop())
        logger.info(
            "Token refresh background task started (interval=%d min)",
            self.interval_minutes,
        )

    def stop(self) -> None:
        """Cancel the background task if it is running."""
        if self._task is not None and not self._task.done():
            self._task.cancel()
            logger.info("Token refresh background task stopped")
        self._task = None

    async def _loop(self) -> None:
        """Internal loop that runs :meth:`run` at the configured interval."""
        while True:
            try:
                await self.run()
            except Exception:
                logger.exception("Unhandled error in token refresh loop")

            await asyncio.sleep(self.interval_minutes * 60)


# ---------------------------------------------------------------------------
# Module-level convenience helpers
# ---------------------------------------------------------------------------

_default_job: TokenRefreshJob | None = None


def start_refresh_job(interval_minutes: int = DEFAULT_INTERVAL_MINUTES) -> TokenRefreshJob:
    """Create and start the default :class:`TokenRefreshJob`.

    Safe to call multiple times — subsequent calls return the existing job.
    """
    global _default_job  # noqa: PLW0603
    if _default_job is None:
        _default_job = TokenRefreshJob(interval_minutes=interval_minutes)
    _default_job.start()
    return _default_job


def stop_refresh_job() -> None:
    """Stop the default :class:`TokenRefreshJob` if running."""
    global _default_job  # noqa: PLW0603
    if _default_job is not None:
        _default_job.stop()
        _default_job = None
