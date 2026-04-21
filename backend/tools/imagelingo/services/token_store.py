from __future__ import annotations

from datetime import datetime, timezone
from backend.db.connection import get_connection


def save_token(handle: str, access_token: str, expires_at: datetime, scopes: str):
    sql = """
        INSERT INTO imagelingo.stores (handle, access_token, expires_at, scopes)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (handle) DO UPDATE
          SET access_token = EXCLUDED.access_token,
              expires_at   = EXCLUDED.expires_at,
              scopes       = EXCLUDED.scopes,
              updated_at   = NOW()
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (handle, access_token, expires_at, scopes))
        conn.commit()


def get_token(handle: str) -> str | None:
    sql = """
        SELECT access_token, expires_at
        FROM imagelingo.stores
        WHERE handle = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (handle,))
            row = cur.fetchone()

    if not row:
        return None
    access_token, expires_at = row
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if expires_at <= datetime.now(timezone.utc):
        return None
    return access_token
