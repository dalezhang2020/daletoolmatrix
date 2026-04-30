"""CRUD operations for shopline_zendesk.bindings table."""

from __future__ import annotations

import logging

from backend.db.connection import get_connection

logger = logging.getLogger(__name__)


def upsert_binding(
    store_id: str,
    zendesk_subdomain: str,
    api_key: str,
) -> dict:
    """Insert a new binding or update on store_id conflict (one-to-one).

    Returns the upserted row as a dict.
    """
    sql = """
        INSERT INTO shopline_zendesk.bindings (store_id, zendesk_subdomain, api_key)
        VALUES (%s, %s, %s)
        ON CONFLICT (store_id) DO UPDATE
          SET zendesk_subdomain = EXCLUDED.zendesk_subdomain,
              api_key           = EXCLUDED.api_key,
              updated_at        = NOW()
        RETURNING id, store_id, zendesk_subdomain, api_key, created_at, updated_at
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (store_id, zendesk_subdomain, api_key))
            row = cur.fetchone()
    return _row_to_dict(row)


def get_binding_by_handle(handle: str) -> dict | None:
    """Look up a binding by Shopline store handle.

    JOINs with shopline_zendesk.stores to resolve handle -> store_id -> binding.
    Returns None if no binding exists for the handle.
    """
    sql = """
        SELECT b.id, b.store_id, b.zendesk_subdomain, b.api_key,
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


def get_binding_by_subdomain(zendesk_subdomain: str) -> dict | None:
    """Look up a binding by Zendesk subdomain.

    JOINs with shopline_zendesk.stores to include the store handle.
    Returns None if no binding exists for the subdomain.
    """
    sql = """
        SELECT b.id, b.store_id, b.zendesk_subdomain, b.api_key,
               b.created_at, b.updated_at, s.handle
        FROM shopline_zendesk.bindings b
        JOIN shopline_zendesk.stores s ON s.id = b.store_id
        WHERE b.zendesk_subdomain = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (zendesk_subdomain,))
            row = cur.fetchone()
    if row is None:
        return None
    return _row_to_dict_with_handle(row)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_COLUMNS = (
    "id",
    "store_id",
    "zendesk_subdomain",
    "api_key",
    "created_at",
    "updated_at",
)

_COLUMNS_WITH_HANDLE = _COLUMNS + ("handle",)


def _row_to_dict(row: tuple) -> dict:
    """Convert a bindings row tuple to a dict keyed by column name."""
    return dict(zip(_COLUMNS, row))


def _row_to_dict_with_handle(row: tuple) -> dict:
    """Convert a bindings+handle row tuple to a dict keyed by column name."""
    return dict(zip(_COLUMNS_WITH_HANDLE, row))
