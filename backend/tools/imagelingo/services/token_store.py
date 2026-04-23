from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta

import httpx

from backend.db.connection import get_connection

logger = logging.getLogger(__name__)


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


def _refresh_token(handle: str, old_token: str) -> str | None:
    """Call Shopline token refresh API. Returns new access_token or None."""
    app_key = os.getenv("SHOPLINE_APP_KEY", "")
    app_secret = os.getenv("SHOPLINE_APP_SECRET", "")
    if not app_key or not app_secret:
        logger.warning("Cannot refresh token: missing SHOPLINE_APP_KEY or SHOPLINE_APP_SECRET")
        return None

    timestamp = str(int(time.time() * 1000))
    body = {"accessToken": old_token}
    body_str = json.dumps(body, separators=(",", ":"))
    source = body_str + timestamp
    sign = hmac.new(
        app_secret.encode("utf-8"),
        source.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    refresh_url = f"https://{handle}.myshopline.com/admin/oauth/token/refresh"
    headers = {
        "Content-Type": "application/json",
        "appkey": app_key,
        "timestamp": timestamp,
        "sign": sign,
    }

    try:
        with httpx.Client(timeout=15) as client:
            resp = client.post(refresh_url, content=body_str, headers=headers)
        data = resp.json()
        logger.info("Token refresh response for %s: code=%s", handle, data.get("code"))

        if data.get("code") != 200 or not data.get("data"):
            logger.warning("Token refresh failed for %s: %s", handle, data.get("message"))
            return None

        token_data = data["data"]
        new_token = token_data.get("accessToken")
        expire_time = token_data.get("expireTime")

        if not new_token:
            return None

        if expire_time:
            try:
                new_expires = datetime.fromisoformat(expire_time)
            except ValueError:
                new_expires = datetime.now(timezone.utc) + timedelta(hours=10)
        else:
            new_expires = datetime.now(timezone.utc) + timedelta(hours=10)

        # Save refreshed token
        save_token(handle, new_token, new_expires, "")
        logger.info("Token refreshed for %s, new expiry: %s", handle, new_expires)
        return new_token

    except Exception as exc:
        logger.error("Token refresh error for %s: %s", handle, exc)
        return None


def get_token(handle: str) -> str | None:
    """Get a valid access token. Auto-refreshes if expired."""
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

    # Token still valid
    if expires_at > datetime.now(timezone.utc):
        return access_token

    # Token expired — try to refresh
    logger.info("Token expired for %s (expired at %s), attempting refresh...", handle, expires_at)
    new_token = _refresh_token(handle, access_token)
    if new_token:
        return new_token

    # Refresh failed — return None (will trigger 401 → re-auth prompt)
    logger.warning("Token refresh failed for %s, user needs to re-authorize", handle)
    return None
