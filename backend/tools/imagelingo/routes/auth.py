import hashlib
import hmac
import logging
import os
import time

import httpx

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse

logger = logging.getLogger(__name__)

router = APIRouter()

SCOPES = "read_products,write_products"


def _env(key: str) -> str:
    return os.getenv(key, "")


def _make_sign(params: dict[str, str]) -> str:
    message = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    return hmac.new(
        _env("SHOPLINE_APP_SECRET").encode(),
        message.encode(),
        hashlib.sha256,
    ).hexdigest()


def verify_hmac(params: dict) -> bool:
    sign = params.get("sign", "")
    filtered = {k: v for k, v in params.items() if k != "sign"}
    expected = _make_sign(filtered)
    return hmac.compare_digest(expected, sign)


@router.get("/install")
async def install(request: Request):
    params = dict(request.query_params)
    handle = params.get("handle", "")

    if os.getenv("SKIP_HMAC_VERIFY", "").lower() not in ("1", "true"):
        if not verify_hmac(params):
            raise HTTPException(status_code=401, detail="Invalid signature")

    app_key = _env("SHOPLINE_APP_KEY")
    redirect_uri = _env("SHOPLINE_REDIRECT_URI")
    auth_url = (
        f"https://{handle}.myshopline.com/admin/oauth-web/#/oauth/authorize"
        f"?appKey={app_key}&responseType=code&scope={SCOPES}&redirectUri={redirect_uri}"
    )
    return RedirectResponse(auth_url)


@router.get("/callback")
@router.get("/callback/")
async def callback(code: str, handle: str):
    app_key = _env("SHOPLINE_APP_KEY")
    app_secret = _env("SHOPLINE_APP_SECRET")
    timestamp = str(int(time.time() * 1000))

    import json as _json
    body = {"code": code}
    body_str = _json.dumps(body, separators=(",", ":"))
    source = body_str + timestamp
    sign = hmac.new(
        app_secret.encode("utf-8"),
        source.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    token_url = f"https://{handle}.myshopline.com/admin/oauth/token/create"
    headers = {
        "Content-Type": "application/json",
        "appkey": app_key,
        "timestamp": timestamp,
        "sign": sign,
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(token_url, content=body_str, headers=headers)

    data = resp.json()
    logger.warning("Shopline token response: %s", data)

    if data.get("code") != 200 or not data.get("data"):
        detail = data.get("message") or data.get("i18nCode") or "Token exchange failed"
        raise HTTPException(status_code=502, detail=detail)

    token_data = data["data"]
    access_token = token_data.get("accessToken")
    expire_time = token_data.get("expireTime")
    scopes = token_data.get("scope", SCOPES)

    if not access_token:
        raise HTTPException(status_code=502, detail="No accessToken in response data")

    from datetime import datetime, timezone, timedelta
    if expire_time:
        try:
            expires_at = datetime.fromisoformat(expire_time)
        except ValueError:
            expires_at = datetime.now(timezone.utc) + timedelta(hours=10)
    else:
        expires_at = datetime.now(timezone.utc) + timedelta(hours=10)

    from backend.tools.imagelingo.services.token_store import save_token
    save_token(handle, access_token, expires_at, scopes)

    from backend.db.connection import get_connection
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM imagelingo.stores WHERE handle = %s", (handle,))
            row = cur.fetchone()
            if row:
                cur.execute(
                    """INSERT INTO imagelingo.subscriptions (store_id, plan, images_limit)
                       VALUES (%s, 'free', 5)
                       ON CONFLICT (store_id) DO NOTHING""",
                    (str(row[0]),),
                )
        conn.commit()

    frontend_url = _env("FRONTEND_URL") or "http://localhost:3000"
    return RedirectResponse(f"{frontend_url}?shop={handle}")


@router.get("/reauth-url")
async def reauth_url(handle: str = ""):
    """Return the OAuth URL for re-authentication when token expires."""
    if not handle:
        from backend.db.connection import get_connection
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT handle FROM imagelingo.stores ORDER BY updated_at DESC LIMIT 1")
                row = cur.fetchone()
        if row:
            handle = row[0]
    if not handle:
        raise HTTPException(400, "No store found. Please install the app first.")
    app_key = _env("SHOPLINE_APP_KEY")
    redirect_uri = _env("SHOPLINE_REDIRECT_URI")
    auth_url = (
        f"https://{handle}.myshopline.com/admin/oauth-web/#/oauth/authorize"
        f"?appKey={app_key}&responseType=code&scope={SCOPES}&redirectUri={redirect_uri}"
    )
    return {"auth_url": auth_url, "handle": handle}
