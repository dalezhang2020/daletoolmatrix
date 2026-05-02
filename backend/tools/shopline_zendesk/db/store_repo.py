"""CRUD operations for shopline_zendesk.stores table."""

from __future__ import annotations

import logging
from datetime import datetime

from backend.db.connection import get_connection

logger = logging.getLogger(__name__)


def upsert_store(
    handle: str,
    access_token: str,
    expires_at: datetime,
    scopes: str | None = None,
) -> dict:
    """Insert a new store or update token on handle conflict.

    Returns the upserted row as a dict.
    """
    sql = """
        INSERT INTO shopline_zendesk.stores (handle, access_token, expires_at, scopes)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (handle) DO UPDATE
          SET access_token = EXCLUDED.access_token,
              expires_at   = EXCLUDED.expires_at,
              scopes       = EXCLUDED.scopes,
              refresh_fail_count = 0,
              token_invalid = FALSE,
              updated_at   = NOW()
        RETURNING id, handle, access_token, expires_at, scopes,
                  installed_at, updated_at, refresh_fail_count, token_invalid
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (handle, access_token, expires_at, scopes))
            row = cur.fetchone()
    return _row_to_dict_extended(row)


def get_store_by_handle(handle: str) -> dict | None:
    """Look up a store by its Shopline handle. Returns None if not found."""
    sql = """
        SELECT id, handle, access_token, expires_at, scopes,
               installed_at, updated_at, refresh_fail_count, token_invalid
        FROM shopline_zendesk.stores
        WHERE handle = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (handle,))
            row = cur.fetchone()
    if row is None:
        return None
    return _row_to_dict_extended(row)


def get_store_by_id(store_id: str) -> dict | None:
    """Look up a store by its UUID. Returns None if not found."""
    sql = """
        SELECT id, handle, access_token, expires_at, scopes,
               installed_at, updated_at, refresh_fail_count, token_invalid
        FROM shopline_zendesk.stores
        WHERE id = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (store_id,))
            row = cur.fetchone()
    if row is None:
        return None
    return _row_to_dict_extended(row)


def update_token(
    handle: str,
    access_token: str,
    expires_at: datetime,
) -> dict | None:
    """Update the access token and expiry for an existing store.

    Returns the updated row as a dict, or None if the handle doesn't exist.
    """
    sql = """
        UPDATE shopline_zendesk.stores
        SET access_token = %s,
            expires_at   = %s,
            updated_at   = NOW()
        WHERE handle = %s
        RETURNING id, handle, access_token, expires_at, scopes, installed_at, updated_at
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (access_token, expires_at, handle))
            row = cur.fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def get_expiring_stores(hours: int = 2) -> list[dict]:
    """Query stores with tokens expiring within *hours* from now.

    Only returns stores where ``token_invalid`` is ``false``.
    """
    sql = """
        SELECT id, handle, access_token, expires_at, scopes,
               installed_at, updated_at, refresh_fail_count, token_invalid
        FROM shopline_zendesk.stores
        WHERE expires_at < NOW() + make_interval(hours => %s)
          AND token_invalid = FALSE
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (hours,))
            rows = cur.fetchall()
    return [_row_to_dict_extended(row) for row in rows]


def increment_refresh_fail_count(handle: str) -> dict | None:
    """Increment ``refresh_fail_count`` by 1 for the given store.

    Returns the updated row as a dict, or None if the handle doesn't exist.
    """
    sql = """
        UPDATE shopline_zendesk.stores
        SET refresh_fail_count = refresh_fail_count + 1,
            updated_at         = NOW()
        WHERE handle = %s
        RETURNING id, handle, access_token, expires_at, scopes,
                  installed_at, updated_at, refresh_fail_count, token_invalid
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (handle,))
            row = cur.fetchone()
    if row is None:
        return None
    return _row_to_dict_extended(row)


def mark_token_invalid(handle: str) -> dict | None:
    """Set ``token_invalid = TRUE`` for the given store.

    Returns the updated row as a dict, or None if the handle doesn't exist.
    """
    sql = """
        UPDATE shopline_zendesk.stores
        SET token_invalid = TRUE,
            updated_at    = NOW()
        WHERE handle = %s
        RETURNING id, handle, access_token, expires_at, scopes,
                  installed_at, updated_at, refresh_fail_count, token_invalid
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (handle,))
            row = cur.fetchone()
    if row is None:
        return None
    return _row_to_dict_extended(row)


def reset_refresh_fail_count(handle: str) -> dict | None:
    """Reset ``refresh_fail_count`` to 0 for the given store.

    Returns the updated row as a dict, or None if the handle doesn't exist.
    """
    sql = """
        UPDATE shopline_zendesk.stores
        SET refresh_fail_count = 0,
            updated_at         = NOW()
        WHERE handle = %s
        RETURNING id, handle, access_token, expires_at, scopes,
                  installed_at, updated_at, refresh_fail_count, token_invalid
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (handle,))
            row = cur.fetchone()
    if row is None:
        return None
    return _row_to_dict_extended(row)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_COLUMNS = (
    "id",
    "handle",
    "access_token",
    "expires_at",
    "scopes",
    "installed_at",
    "updated_at",
)

_COLUMNS_EXTENDED = _COLUMNS + (
    "refresh_fail_count",
    "token_invalid",
)


def _row_to_dict(row: tuple) -> dict:
    """Convert a stores row tuple to a dict keyed by column name."""
    return dict(zip(_COLUMNS, row))


def _row_to_dict_extended(row: tuple) -> dict:
    """Convert a stores row tuple (with refresh columns) to a dict."""
    return dict(zip(_COLUMNS_EXTENDED, row))
