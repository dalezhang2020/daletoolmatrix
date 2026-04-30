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
              updated_at   = NOW()
        RETURNING id, handle, access_token, expires_at, scopes, installed_at, updated_at
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (handle, access_token, expires_at, scopes))
            row = cur.fetchone()
    return _row_to_dict(row)


def get_store_by_handle(handle: str) -> dict | None:
    """Look up a store by its Shopline handle. Returns None if not found."""
    sql = """
        SELECT id, handle, access_token, expires_at, scopes, installed_at, updated_at
        FROM shopline_zendesk.stores
        WHERE handle = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (handle,))
            row = cur.fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def get_store_by_id(store_id: str) -> dict | None:
    """Look up a store by its UUID. Returns None if not found."""
    sql = """
        SELECT id, handle, access_token, expires_at, scopes, installed_at, updated_at
        FROM shopline_zendesk.stores
        WHERE id = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (store_id,))
            row = cur.fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


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


def _row_to_dict(row: tuple) -> dict:
    """Convert a stores row tuple to a dict keyed by column name."""
    return dict(zip(_COLUMNS, row))
