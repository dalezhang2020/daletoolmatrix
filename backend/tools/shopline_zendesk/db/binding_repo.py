"""CRUD operations for shopline_zendesk.bindings table."""

from __future__ import annotations

import logging
from datetime import datetime

from backend.db.connection import get_connection

logger = logging.getLogger(__name__)


def upsert_binding(
    store_id: str,
    zendesk_subdomain: str,
    api_key: str,
    zendesk_admin_email: str | None = None,
    zendesk_api_token: str | None = None,
    zendesk_access_token: str | None = None,
    zendesk_refresh_token: str | None = None,
    zendesk_token_expires_at: datetime | None = None,
) -> dict:
    """Insert a new binding or update on store_id conflict (one-to-one).

    Returns the upserted row as a dict (includes computed
    ``has_zendesk_credentials`` key).
    """
    sql = """
        INSERT INTO shopline_zendesk.bindings
            (store_id, zendesk_subdomain, api_key,
             zendesk_admin_email, zendesk_api_token,
             zendesk_access_token, zendesk_refresh_token, zendesk_token_expires_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (store_id) DO UPDATE
          SET zendesk_subdomain        = EXCLUDED.zendesk_subdomain,
              api_key                  = EXCLUDED.api_key,
              zendesk_admin_email      = EXCLUDED.zendesk_admin_email,
              zendesk_api_token        = EXCLUDED.zendesk_api_token,
              zendesk_access_token     = EXCLUDED.zendesk_access_token,
              zendesk_refresh_token    = EXCLUDED.zendesk_refresh_token,
              zendesk_token_expires_at = EXCLUDED.zendesk_token_expires_at,
              updated_at               = NOW()
        RETURNING id, store_id, zendesk_subdomain, api_key,
                  zendesk_admin_email, zendesk_api_token,
                  zendesk_access_token, zendesk_refresh_token,
                  zendesk_token_expires_at,
                  created_at, updated_at
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (store_id, zendesk_subdomain, api_key,
                 zendesk_admin_email, zendesk_api_token,
                 zendesk_access_token, zendesk_refresh_token,
                 zendesk_token_expires_at),
            )
            row = cur.fetchone()
    return _row_to_dict(row)


def update_zendesk_tokens(
    store_id: str,
    zendesk_access_token: str,
    zendesk_refresh_token: str | None = None,
    zendesk_token_expires_at: datetime | None = None,
) -> dict | None:
    """Update only the Zendesk OAuth tokens for an existing binding.

    Returns the updated row as a dict, or None if no binding exists.
    """
    sql = """
        UPDATE shopline_zendesk.bindings
          SET zendesk_access_token     = %s,
              zendesk_refresh_token    = %s,
              zendesk_token_expires_at = %s,
              updated_at               = NOW()
        WHERE store_id = %s
        RETURNING id, store_id, zendesk_subdomain, api_key,
                  zendesk_admin_email, zendesk_api_token,
                  zendesk_access_token, zendesk_refresh_token,
                  zendesk_token_expires_at,
                  created_at, updated_at
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (zendesk_access_token, zendesk_refresh_token,
                 zendesk_token_expires_at, store_id),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def get_binding_by_handle(handle: str) -> dict | None:
    """Look up a binding by Shopline store handle.

    JOINs with shopline_zendesk.stores to resolve handle -> store_id -> binding.
    Returns None if no binding exists for the handle.
    """
    sql = """
        SELECT b.id, b.store_id, b.zendesk_subdomain, b.api_key,
               b.zendesk_admin_email, b.zendesk_api_token,
               b.zendesk_access_token, b.zendesk_refresh_token,
               b.zendesk_token_expires_at,
               b.created_at, b.updated_at, s.handle
        FROM shopline_zendesk.bindings b
        JOIN shopline_zendesk.stores s ON s.id = b.store_id
        WHERE s.handle = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (handle,))
            row = cur.fetchone()
    if row is None:
        return None
    return _row_to_dict_with_handle(row)


def get_binding_by_subdomain_and_handle(
    zendesk_subdomain: str,
    handle: str,
) -> dict | None:
    """Look up a binding by Zendesk subdomain and Shopline handle."""
    sql = """
        SELECT b.id, b.store_id, b.zendesk_subdomain, b.api_key,
               b.zendesk_admin_email, b.zendesk_api_token,
               b.zendesk_access_token, b.zendesk_refresh_token,
               b.zendesk_token_expires_at,
               b.created_at, b.updated_at, s.handle
        FROM shopline_zendesk.bindings b
        JOIN shopline_zendesk.stores s ON s.id = b.store_id
        WHERE b.zendesk_subdomain = %s
          AND s.handle = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (zendesk_subdomain, handle))
            row = cur.fetchone()
    if row is None:
        return None
    return _row_to_dict_with_handle(row)


def delete_binding_by_subdomain(zendesk_subdomain: str) -> bool:
    """Delete a binding by Zendesk subdomain. Returns True if a row was deleted.

    The associated store record is intentionally kept.
    """
    sql = """
        DELETE FROM shopline_zendesk.bindings
        WHERE zendesk_subdomain = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (zendesk_subdomain,))
            return cur.rowcount > 0


def delete_binding_by_subdomain_and_handle(
    zendesk_subdomain: str,
    handle: str,
) -> bool:
    """Delete a binding by Zendesk subdomain and store handle."""
    sql = """
        DELETE FROM shopline_zendesk.bindings b
        USING shopline_zendesk.stores s
        WHERE b.store_id = s.id
          AND b.zendesk_subdomain = %s
          AND s.handle = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (zendesk_subdomain, handle))
            return cur.rowcount > 0


def get_binding_by_subdomain(zendesk_subdomain: str) -> dict | None:
    """Look up a binding by Zendesk subdomain.

    JOINs with shopline_zendesk.stores to include the store handle.
    This is retained for legacy single-store callers and returns the most
    recently updated matching binding when multiple stores are linked.
    """
    sql = """
        SELECT b.id, b.store_id, b.zendesk_subdomain, b.api_key,
               b.zendesk_admin_email, b.zendesk_api_token,
               b.zendesk_access_token, b.zendesk_refresh_token,
               b.zendesk_token_expires_at,
               b.created_at, b.updated_at, s.handle
        FROM shopline_zendesk.bindings b
        JOIN shopline_zendesk.stores s ON s.id = b.store_id
        WHERE b.zendesk_subdomain = %s
        ORDER BY b.updated_at DESC, s.handle ASC
        LIMIT 1
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (zendesk_subdomain,))
            row = cur.fetchone()
    if row is None:
        return None
    return _row_to_dict_with_handle(row)


def list_bindings_by_subdomain(zendesk_subdomain: str) -> list[dict]:
    """List every binding linked to the Zendesk subdomain."""
    sql = """
        SELECT b.id, b.store_id, b.zendesk_subdomain, b.api_key,
               b.zendesk_admin_email, b.zendesk_api_token,
               b.zendesk_access_token, b.zendesk_refresh_token,
               b.zendesk_token_expires_at,
               b.created_at, b.updated_at, s.handle
        FROM shopline_zendesk.bindings b
        JOIN shopline_zendesk.stores s ON s.id = b.store_id
        WHERE b.zendesk_subdomain = %s
        ORDER BY b.updated_at DESC, s.handle ASC
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (zendesk_subdomain,))
            rows = cur.fetchall()
    return [_row_to_dict_with_handle(row) for row in rows]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_COLUMNS = (
    "id",
    "store_id",
    "zendesk_subdomain",
    "api_key",
    "zendesk_admin_email",
    "zendesk_api_token",
    "zendesk_access_token",
    "zendesk_refresh_token",
    "zendesk_token_expires_at",
    "created_at",
    "updated_at",
)

_COLUMNS_WITH_HANDLE = _COLUMNS + ("handle",)


def _has_zendesk_credentials(row_dict: dict) -> bool:
    """Return True if Zendesk admin email AND API token are both set."""
    return bool(row_dict.get("zendesk_admin_email")) and bool(row_dict.get("zendesk_api_token"))


def _row_to_dict(row: tuple) -> dict:
    """Convert a bindings row tuple to a dict keyed by column name."""
    d = dict(zip(_COLUMNS, row))
    d["has_zendesk_credentials"] = _has_zendesk_credentials(d)
    return d


def _row_to_dict_with_handle(row: tuple) -> dict:
    """Convert a bindings+handle row tuple to a dict keyed by column name."""
    d = dict(zip(_COLUMNS_WITH_HANDLE, row))
    d["has_zendesk_credentials"] = _has_zendesk_credentials(d)
    return d
