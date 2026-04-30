"""Zendesk OAuth 2.0 Authorization Code Flow routes.

GET /zendesk/authorize  — Redirect user to Zendesk authorization page
GET /zendesk/callback   — Exchange authorization code for tokens
"""

from __future__ import annotations

import base64
import json
import logging
import os
import urllib.parse

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse

from backend.tools.shopline_zendesk.services import binding_service

logger = logging.getLogger(__name__)

router = APIRouter()

_TOKEN_TIMEOUT = 15.0  # seconds


def _env(key: str) -> str:
    """Return an environment variable or empty string."""
    return os.environ.get(key, "")


def _build_redirect_uri() -> str:
    """Build the fixed OAuth callback URL."""
    base_url = _env("SHOPLINE_ZD_APP_URL").rstrip("/")
    return f"{base_url}/api/shopline-zendesk/shopline/zendesk/callback"


# ---------------------------------------------------------------------------
# GET /zendesk/authorize — Initiate Zendesk OAuth
# ---------------------------------------------------------------------------


@router.get("/zendesk/authorize")
async def zendesk_authorize(
    handle: str = Query(..., min_length=1, max_length=100),
    subdomain: str = Query(..., min_length=1, max_length=63, pattern=r"^[a-z0-9-]+$"),
):
    """Redirect the user to the Zendesk OAuth authorization page.

    The ``state`` parameter carries ``{handle, subdomain}`` as base64-encoded
    JSON so the callback can associate the grant with the correct store.

    Since the Shopline app runs inside an iframe, we use a JS redirect on
    ``window.top`` to break out of the iframe for the Zendesk auth page.
    """
    client_id = _env("ZENDESK_CLIENT_ID")
    if not client_id:
        raise HTTPException(status_code=500, detail="ZENDESK_CLIENT_ID not configured")

    redirect_uri = _build_redirect_uri()

    # Encode handle + subdomain into the state param (strip padding for URL safety)
    state_payload = json.dumps({"handle": handle, "subdomain": subdomain})
    state = base64.urlsafe_b64encode(state_payload.encode()).decode().rstrip("=")

    auth_url = (
        f"https://{subdomain}.zendesk.com/oauth/authorizations/new"
        f"?response_type=code"
        f"&redirect_uri={urllib.parse.quote(redirect_uri, safe='')}"
        f"&client_id={urllib.parse.quote(client_id, safe='')}"
        f"&scope=read"
        f"&state={urllib.parse.quote(state, safe='')}"
    )

    # Use JS to redirect the top-level window (break out of Shopline iframe)
    return HTMLResponse(
        f'<!DOCTYPE html><html><head><title>Redirecting to Zendesk…</title></head>'
        f'<body><script>window.top.location.href = "{auth_url}";</script>'
        f'<p>Redirecting to Zendesk authorization…</p></body></html>'
    )


# ---------------------------------------------------------------------------
# GET /zendesk/callback — Zendesk OAuth callback
# ---------------------------------------------------------------------------


@router.get("/zendesk/callback")
async def zendesk_callback(
    code: str = Query(..., min_length=1),
    state: str = Query(..., min_length=1),
):
    """Handle the Zendesk OAuth callback.

    1. Decode the ``state`` param to recover handle + subdomain.
    2. Exchange the authorization code for access + refresh tokens.
    3. Store tokens in the bindings table.
    4. Redirect back to the Shopline admin app page so the iframe reloads.
    """
    # --- Decode state (add back padding stripped during encode) ---
    try:
        padded = state + "=" * (-len(state) % 4)
        state_json = base64.urlsafe_b64decode(padded.encode()).decode()
        state_data = json.loads(state_json)
        handle = state_data["handle"]
        subdomain = state_data["subdomain"]
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid state parameter")

    if not handle or not subdomain:
        raise HTTPException(status_code=400, detail="Missing handle or subdomain in state")

    # --- Exchange code for tokens ---
    client_id = _env("ZENDESK_CLIENT_ID")
    client_secret = _env("ZENDESK_CLIENT_SECRET")
    redirect_uri = _build_redirect_uri()

    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="Zendesk OAuth client not configured")

    token_url = f"https://{subdomain}.zendesk.com/oauth/tokens"
    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "scope": "read",
    }

    try:
        async with httpx.AsyncClient(timeout=_TOKEN_TIMEOUT) as client:
            resp = await client.post(
                token_url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        logger.error(
            "Zendesk token exchange failed for handle=%s subdomain=%s status=%s: %s",
            handle, subdomain, exc.response.status_code, exc.response.text,
        )
        raise HTTPException(
            status_code=502,
            detail=f"Zendesk token exchange failed: {exc.response.status_code}",
        ) from exc
    except httpx.TimeoutException as exc:
        logger.error("Zendesk token exchange timeout for handle=%s", handle)
        raise HTTPException(
            status_code=502,
            detail="Zendesk token exchange timed out",
        ) from exc

    token_data = resp.json()
    access_token = token_data.get("access_token", "")
    refresh_token = token_data.get("refresh_token")

    if not access_token:
        logger.error("Zendesk returned empty access_token for handle=%s", handle)
        raise HTTPException(status_code=502, detail="Zendesk returned empty access token")

    # --- Store tokens ---
    try:
        binding_service.update_zendesk_tokens(
            handle=handle,
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=None,  # Zendesk OAuth tokens don't expire by default
        )
    except binding_service.StoreNotFoundError:
        raise HTTPException(status_code=404, detail="Store not found")

    logger.info("Zendesk OAuth tokens stored for handle=%s subdomain=%s", handle, subdomain)

    # --- Redirect back to Shopline admin app ---
    # The Shopline admin will reload the app iframe, which hits /entry → frontend.
    app_key = _env("SHOPLINE_ZD_APP_KEY")
    return RedirectResponse(
        f"https://{handle}.myshopline.com/admin/apps/detail/{app_key}"
        f"?zendesk_connected=true"
    )
