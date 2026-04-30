"""Shopline HMAC verification, token exchange, and token refresh."""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
from datetime import datetime, timezone

import httpx

from backend.tools.shopline_zendesk.config import get_skip_hmac

logger = logging.getLogger(__name__)

_TIMEOUT = 15.0  # seconds for all Shopline API calls


# ---------------------------------------------------------------------------
# HMAC signature helpers
# ---------------------------------------------------------------------------


def _make_sign(params: dict, secret: str) -> str:
    """Compute the HMAC-SHA256 signature string for *params*.

    Algorithm (Shopline standard):
    1. Remove the ``sign`` key from the parameter dict.
    2. Sort remaining keys alphabetically.
    3. Concatenate as ``key=value`` pairs (no separator between pairs).
    4. HMAC-SHA256 the concatenated string with *secret*, return hex digest.
    """
    filtered = {k: v for k, v in params.items() if k != "sign"}
    sorted_keys = sorted(filtered.keys())
    message = "&".join(f"{k}={filtered[k]}" for k in sorted_keys)
    return hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def verify_hmac(params: dict, secret: str) -> bool:
    """Verify the HMAC-SHA256 signature carried in *params['sign']*.

    Returns ``True`` when:
    - ``SHOPLINE_ZD_SKIP_HMAC`` is enabled (local dev), **or**
    - the computed signature matches ``params['sign']`` (constant-time compare).

    Returns ``False`` when the ``sign`` key is missing or the comparison fails.
    """
    if get_skip_hmac():
        logger.debug("HMAC verification skipped (SHOPLINE_ZD_SKIP_HMAC=1)")
        return True

    provided = params.get("sign")
    if not provided:
        logger.warning("HMAC verification failed: 'sign' param missing")
        return False

    expected = _make_sign(params, secret)
    ok = hmac.compare_digest(expected, provided)
    if not ok:
        logger.warning("HMAC verification failed: signature mismatch")
    return ok


# ---------------------------------------------------------------------------
# Token exchange & refresh
# ---------------------------------------------------------------------------


async def exchange_code_for_token(
    handle: str,
    code: str,
) -> tuple[str, datetime, str]:
    """Exchange an OAuth authorization *code* for an access token.

    Calls ``POST https://{handle}.myshopline.com/admin/oauth/token/create``.

    Shopline POST signing: sign = HMAC-SHA256(body_string + timestamp, app_secret)
    Headers must include: appkey, timestamp, sign, Content-Type.

    Returns:
        A tuple of ``(access_token, expires_at, scopes)``.

    Raises:
        httpx.HTTPStatusError: If the Shopline API returns a non-2xx status.
        httpx.TimeoutException: If the request exceeds the timeout.
    """
    import json as _json
    import time

    app_key = os.environ["SHOPLINE_ZD_APP_KEY"]
    app_secret = os.environ["SHOPLINE_ZD_APP_SECRET"]

    url = f"https://{handle}.myshopline.com/admin/oauth/token/create"

    # Build body and POST signature (body_string + timestamp)
    body = {"code": code}
    body_str = _json.dumps(body, separators=(",", ":"))
    timestamp = str(int(time.time() * 1000))
    source = body_str + timestamp
    sign = hmac.new(
        app_secret.encode("utf-8"),
        source.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    headers = {
        "Content-Type": "application/json",
        "appkey": app_key,
        "timestamp": timestamp,
        "sign": sign,
    }

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.post(url, content=body_str, headers=headers)
        resp.raise_for_status()

    data = resp.json()
    logger.info("Token response for handle=%s: code=%s", handle, data.get("code"))

    if data.get("code") != 200 or not data.get("data"):
        detail = data.get("message") or data.get("i18nCode") or "Token exchange failed"
        raise RuntimeError(detail)

    token_data = data["data"]
    access_token: str = token_data["accessToken"]
    expire_time_str = token_data.get("expireTime", "")
    scopes: str = token_data.get("scope", "")

    # expireTime is ISO format string, not epoch seconds
    from datetime import timedelta
    if expire_time_str:
        try:
            expires_at = datetime.fromisoformat(expire_time_str)
        except ValueError:
            expires_at = datetime.now(timezone.utc) + timedelta(hours=10)
    else:
        expires_at = datetime.now(timezone.utc) + timedelta(hours=10)

    logger.info("Token exchanged for handle=%s, expires_at=%s", handle, expires_at)
    return access_token, expires_at, scopes


async def refresh_token(
    handle: str,
    old_token: str,
) -> tuple[str, datetime]:
    """Refresh an expired access token.

    Calls ``POST https://{handle}.myshopline.com/admin/oauth/token/refresh``.
    Uses the same POST signing as token create.

    Returns:
        A tuple of ``(new_access_token, expires_at)``.

    Raises:
        httpx.HTTPStatusError: If the Shopline API returns a non-2xx status.
        httpx.TimeoutException: If the request exceeds the timeout.
    """
    import json as _json
    import time
    from datetime import timedelta

    app_key = os.environ["SHOPLINE_ZD_APP_KEY"]
    app_secret = os.environ["SHOPLINE_ZD_APP_SECRET"]

    url = f"https://{handle}.myshopline.com/admin/oauth/token/refresh"

    body = {"accessToken": old_token}
    body_str = _json.dumps(body, separators=(",", ":"))
    timestamp = str(int(time.time() * 1000))
    source = body_str + timestamp
    sign = hmac.new(
        app_secret.encode("utf-8"),
        source.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    headers = {
        "Content-Type": "application/json",
        "appkey": app_key,
        "timestamp": timestamp,
        "sign": sign,
    }

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.post(url, content=body_str, headers=headers)
        resp.raise_for_status()

    data = resp.json()
    if data.get("code") != 200 or not data.get("data"):
        detail = data.get("message") or data.get("i18nCode") or "Token refresh failed"
        raise RuntimeError(detail)

    token_data = data["data"]
    new_token: str = token_data["accessToken"]
    expire_time_str = token_data.get("expireTime", "")

    if expire_time_str:
        try:
            expires_at = datetime.fromisoformat(expire_time_str)
        except ValueError:
            expires_at = datetime.now(timezone.utc) + timedelta(hours=10)
    else:
        expires_at = datetime.now(timezone.utc) + timedelta(hours=10)

    logger.info("Token refreshed for handle=%s, expires_at=%s", handle, expires_at)
    return new_token, expires_at
