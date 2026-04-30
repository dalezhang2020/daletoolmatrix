"""Shopline OAuth installation routes: GET /install, GET /callback."""

from __future__ import annotations

import logging
import os
import urllib.parse

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse

from backend.tools.shopline_zendesk.db import store_repo
from backend.tools.shopline_zendesk.services import shopline_auth

logger = logging.getLogger(__name__)

router = APIRouter()

SCOPES = "read_customers"


def _env(key: str) -> str:
    """Return an environment variable or empty string."""
    return os.environ.get(key, "")


# ---------------------------------------------------------------------------
# GET /entry — App entry point (Shopline loads this URL every time)
# ---------------------------------------------------------------------------


@router.get("/entry")
async def entry(request: Request):
    """Shopline loads this URL when merchant opens the app.

    - Verify HMAC signature (required by Shopline for every request)
    - If store already authorized → redirect to Vercel frontend
    - If not authorized → redirect top window to OAuth page (avoid iframe nesting)
    """
    from fastapi.responses import HTMLResponse

    params = dict(request.query_params)
    handle = params.get("handle", "")

    if not shopline_auth.verify_hmac(params, _env("SHOPLINE_ZD_APP_SECRET")):
        logger.warning("Entry HMAC verification failed for handle=%s", handle)
        raise HTTPException(status_code=401, detail="Invalid signature")

    # Check if store already has a valid token
    store = store_repo.get_store_by_handle(handle) if handle else None

    if store:
        # Already authorized → load frontend inside the Shopline iframe
        frontend_url = _env("SHOPLINE_ZD_FRONTEND_URL") or "http://localhost:3000"
        target_url = f"{frontend_url}?handle={handle}"
        # Use JS redirect so the iframe loads the frontend content directly
        return HTMLResponse(
            f'<!DOCTYPE html><html><head><title>Loading...</title></head>'
            f'<body><script>window.location.href = "{target_url}";</script>'
            f'<p>Loading app...</p></body></html>'
        )

    # Not authorized → break out of iframe to OAuth page
    # Use JS to redirect top window to avoid nested Shopline admin sidebars
    app_key = _env("SHOPLINE_ZD_APP_KEY")
    # Use fixed callback URL (request.url_for may generate wrong scheme/host behind proxy)
    base_url = _env("SHOPLINE_ZD_APP_URL") or str(request.base_url).rstrip("/")
    callback_url = f"{base_url}/api/shopline-zendesk/shopline/callback"
    redirect_uri = urllib.parse.quote(callback_url, safe="")
    auth_url = (
        f"https://{handle}.myshopline.com/admin/oauth-web/#/oauth/authorize"
        f"?appKey={app_key}&responseType=code&scope={SCOPES}&redirectUri={redirect_uri}"
    )
    return HTMLResponse(
        f'<!DOCTYPE html><html><head><title>Redirecting...</title></head>'
        f'<body><script>'
        f'try {{ window.top.location.href = "{auth_url}"; }}'
        f' catch(e) {{ try {{ window.parent.location.href = "{auth_url}"; }}'
        f' catch(e2) {{ window.location.href = "{auth_url}"; }} }}'
        f'</script>'
        f'<p>Redirecting to authorization...</p></body></html>'
    )


# ---------------------------------------------------------------------------
# GET /install — OAuth entry point (first install)
# ---------------------------------------------------------------------------


@router.get("/install")
async def install(request: Request):
    """Shopline sends merchants here when they click 'Install'.

    Verify the HMAC-SHA256 signature, then redirect to the Shopline OAuth
    authorization page so the merchant can grant access.
    """
    params = dict(request.query_params)
    handle = params.get("handle", "")

    if not shopline_auth.verify_hmac(params, _env("SHOPLINE_ZD_APP_SECRET")):
        logger.warning("Install HMAC verification failed for handle=%s", handle)
        raise HTTPException(status_code=401, detail="Invalid signature")

    app_key = _env("SHOPLINE_ZD_APP_KEY")
    # Use fixed callback URL (request.url_for may generate wrong scheme/host behind proxy)
    base_url = _env("SHOPLINE_ZD_APP_URL") or str(request.base_url).rstrip("/")
    callback_url = f"{base_url}/api/shopline-zendesk/shopline/callback"
    redirect_uri = urllib.parse.quote(callback_url, safe="")

    auth_url = (
        f"https://{handle}.myshopline.com/admin/oauth-web/#/oauth/authorize"
        f"?appKey={app_key}"
        f"&responseType=code"
        f"&redirectUri={redirect_uri}"
        f"&scope={SCOPES}"
    )
    return RedirectResponse(auth_url)


# ---------------------------------------------------------------------------
# GET /callback — OAuth callback
# ---------------------------------------------------------------------------


@router.get("/callback")
@router.get("/callback/")
async def callback(request: Request):
    """OAuth callback: Shopline redirects here with an authorization code.

    Verify the HMAC signature, exchange the code for an access token,
    persist the token, and redirect to the Shopline frontend.
    """
    params = dict(request.query_params)
    handle = params.get("handle", "")
    code = params.get("code", "")

    if not handle or not code:
        raise HTTPException(status_code=400, detail="Missing handle or code")

    # Verify callback signature
    if not shopline_auth.verify_hmac(params, _env("SHOPLINE_ZD_APP_SECRET")):
        logger.warning("Callback HMAC verification failed for handle=%s", handle)
        raise HTTPException(status_code=401, detail="Invalid signature")

    # Exchange authorization code for access token
    try:
        access_token, expires_at, scopes = await shopline_auth.exchange_code_for_token(
            handle, code
        )
    except httpx.HTTPStatusError as exc:
        logger.error(
            "Token exchange HTTP error for handle=%s: %s %s",
            handle,
            exc.response.status_code,
            exc.response.text,
        )
        raise HTTPException(
            status_code=502,
            detail=f"Token exchange failed: {exc.response.status_code}",
        ) from exc
    except httpx.TimeoutException as exc:
        logger.error("Token exchange timeout for handle=%s", handle)
        raise HTTPException(
            status_code=502,
            detail="Upstream timeout during token exchange",
        ) from exc
    except RuntimeError as exc:
        logger.error("Token exchange error for handle=%s: %s", handle, exc)
        raise HTTPException(
            status_code=502,
            detail=str(exc),
        ) from exc

    # Persist token to database
    store_repo.upsert_store(
        handle=handle,
        access_token=access_token,
        expires_at=expires_at,
        scopes=scopes,
    )
    logger.info("Store upserted for handle=%s", handle)

    # Redirect back to the Shopline admin app page.
    # Shopline will reload the app iframe, hitting /entry again.
    # This time /entry finds a valid token and loads the frontend inside the iframe.
    # This avoids iframe nesting issues that occur when redirecting directly to Vercel.
    return RedirectResponse(
        f"https://{handle}.myshopline.com/admin/apps/detail/{_env('SHOPLINE_ZD_APP_KEY')}"
    )
