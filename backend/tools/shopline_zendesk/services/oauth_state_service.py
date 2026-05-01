"""Manage OAuth state parameters for CSRF protection."""

from __future__ import annotations

import logging
import secrets
from datetime import datetime, timedelta, timezone

from backend.tools.shopline_zendesk.db import oauth_state_repo

logger = logging.getLogger(__name__)

# State token length in bytes (produces 64 hex chars, well above the 32-char minimum).
_STATE_BYTES = 32

# Time-to-live for an OAuth state record.
_STATE_TTL = timedelta(minutes=10)


def generate_state(zendesk_subdomain: str, handle: str) -> str:
    """Generate a cryptographically secure OAuth state and persist it.

    The state is a 64-character hex string (32 random bytes) stored in the
    ``oauth_states`` table with a 10-minute TTL.

    Returns:
        The generated state string.
    """
    state = secrets.token_hex(_STATE_BYTES)
    expires_at = datetime.now(timezone.utc) + _STATE_TTL

    oauth_state_repo.create_state(
        state=state,
        zendesk_subdomain=zendesk_subdomain,
        handle=handle,
        expires_at=expires_at,
    )
    logger.info(
        "OAuth state generated for subdomain=%s handle=%s (expires %s)",
        zendesk_subdomain,
        handle,
        expires_at.isoformat(),
    )
    return state


def verify_state(state: str, handle: str) -> bool:
    """Verify an OAuth state value.

    Returns ``True`` only when:
    1. A record with the given *state* exists in the database.
    2. The stored ``handle`` matches the provided *handle*.
    3. The record has not expired (``expires_at`` is in the future).

    On successful verification the state record is deleted to prevent reuse.
    """
    record = oauth_state_repo.get_state(state)
    if record is None:
        logger.warning("OAuth state verification failed: state not found")
        return False

    if record["handle"] != handle:
        logger.warning(
            "OAuth state verification failed: handle mismatch "
            "(expected=%s, got=%s)",
            record["handle"],
            handle,
        )
        return False

    # Compare expiry against current UTC time.
    expires_at = record["expires_at"]
    now = datetime.now(timezone.utc)

    # Make expires_at offset-aware if the DB returns a naive timestamp.
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    if now >= expires_at:
        logger.warning(
            "OAuth state verification failed: state expired at %s",
            expires_at.isoformat(),
        )
        # Clean up the expired record.
        oauth_state_repo.delete_state(state)
        return False

    # Valid — delete to prevent replay.
    oauth_state_repo.delete_state(state)
    logger.info("OAuth state verified and consumed for handle=%s", handle)
    return True


def cleanup_expired_states() -> int:
    """Delete all expired OAuth state records.

    Returns:
        The number of records deleted.
    """
    count = oauth_state_repo.cleanup_expired_states()
    if count:
        logger.info("Cleaned up %d expired OAuth state(s)", count)
    return count
