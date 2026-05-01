"""CRUD operations for shopline_zendesk.oauth_states table."""

from __future__ import annotations

import logging
from datetime import datetime

from backend.db.connection import get_connection

logger = logging.getLogger(__name__)


def create_state(
    state: str,
    zendesk_subdomain: str,
    handle: str,
    expires_at: datetime,
) -> dict:
    """Insert a new OAuth state record for CSRF protection.

    Returns the inserted row as a dict.
    """
    sql = """
        INSERT INTO shopline_zendesk.oauth_states
            (state, zendesk_subdomain, handle, expires_at)
        VALUES (%s, %s, %s, %s)
        RETURNING id, state, zendesk_subdomain, handle, created_at, expires_at
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (state, zendesk_subdomain, handle, expires_at))
            row = cur.fetchone()
    return _row_to_dict(row)


def get_state(state: str) -> dict | None:
    """Look up an OAuth state by its value. Returns None if not found."""
    sql = """
        SELECT id, state, zendesk_subdomain, handle, created_at, expires_at
        FROM shopline_zendesk.oauth_states
        WHERE state = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (state,))
            row = cur.fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def delete_state(state: str) -> bool:
    """Delete an OAuth state by its value. Returns True if a row was deleted."""
    sql = """
        DELETE FROM shopline_zendesk.oauth_states
        WHERE state = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (state,))
            return cur.rowcount > 0


def cleanup_expired_states() -> int:
    """Delete all expired OAuth states. Returns the number of rows deleted."""
    sql = """
        DELETE FROM shopline_zendesk.oauth_states
        WHERE expires_at < NOW()
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.rowcount


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_COLUMNS = (
    "id",
    "state",
    "zendesk_subdomain",
    "handle",
    "created_at",
    "expires_at",
)


def _row_to_dict(row: tuple) -> dict:
    """Convert an oauth_states row tuple to a dict keyed by column name."""
    return dict(zip(_COLUMNS, row))
