"""OAuth routes for Shopline-Zendesk integration.

Handles the OAuth popup flow:
- GET  /oauth/shopline/start       — render handle input form
- POST /oauth/shopline/start       — validate, generate state, redirect to Shopline
- GET  /oauth/shopline/callback    — handle Shopline OAuth callback
- POST /oauth/shopline/disconnect  — remove binding for a Zendesk subdomain
"""

from __future__ import annotations

import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from html import escape

import httpx
from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from backend.tools.shopline_zendesk.db import binding_repo, oauth_state_repo, store_repo
from backend.tools.shopline_zendesk.services import shopline_auth
from backend.tools.shopline_zendesk.services.oauth_state_service import (
    generate_state,
    verify_state,
)
from backend.tools.shopline_zendesk.services.validators import (
    build_shopline_auth_url,
    validate_handle,
    validate_zendesk_subdomain,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/oauth/shopline", tags=["shopline-oauth"], redirect_slashes=True)


# ---------------------------------------------------------------------------
# Rate limiting (in-memory, per zendesk_subdomain)
# ---------------------------------------------------------------------------

_RATE_LIMIT_MAX = 10  # requests per window
_RATE_LIMIT_WINDOW = 60  # seconds
_rate_limit_store: dict[str, list[float]] = defaultdict(list)


def _check_rate_limit(zendesk_subdomain: str) -> bool:
    """Return True if the request is within the rate limit, False otherwise."""
    now = time.monotonic()
    window_start = now - _RATE_LIMIT_WINDOW

    # Prune old timestamps.
    timestamps = _rate_limit_store[zendesk_subdomain]
    _rate_limit_store[zendesk_subdomain] = [
        ts for ts in timestamps if ts > window_start
    ]

    if len(_rate_limit_store[zendesk_subdomain]) >= _RATE_LIMIT_MAX:
        return False

    _rate_limit_store[zendesk_subdomain].append(now)
    return True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _env(key: str) -> str:
    """Return an environment variable or empty string."""
    return os.environ.get(key, "")


def _render_error_html(
    title: str,
    message: str,
    retry_url: str | None = None,
) -> HTMLResponse:
    """Render a simple error HTML page for the popup window."""
    retry_btn = ""
    if retry_url:
        retry_btn = (
            f'<a href="{escape(retry_url)}" '
            f'style="display:inline-block;margin-top:16px;padding:10px 24px;'
            f'background:#2563eb;color:#fff;border-radius:6px;text-decoration:none;">'
            f"Retry</a>"
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>{escape(title)}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif;
         display: flex; align-items: center; justify-content: center;
         min-height: 100vh; margin: 0; background: #f9fafb; }}
  .card {{ background: #fff; border-radius: 12px; padding: 32px;
           box-shadow: 0 1px 3px rgba(0,0,0,.1); max-width: 420px;
           text-align: center; }}
  h1 {{ font-size: 20px; color: #dc2626; margin-bottom: 12px; }}
  p {{ color: #6b7280; line-height: 1.5; }}
</style>
</head>
<body>
  <div class="card">
    <h1>{escape(title)}</h1>
    <p>{escape(message)}</p>
    {retry_btn}
  </div>
</body>
</html>"""
    return HTMLResponse(content=html)


def _render_success_html(zendesk_subdomain: str) -> HTMLResponse:
    """Render an HTML page that notifies the opener and closes the popup."""
    safe_subdomain = escape(zendesk_subdomain)
    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>Authorization Complete</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif;
         display: flex; align-items: center; justify-content: center;
         min-height: 100vh; margin: 0; background: #f9fafb; }}
  .card {{ background: #fff; border-radius: 12px; padding: 32px;
           box-shadow: 0 1px 3px rgba(0,0,0,.1); max-width: 420px;
           text-align: center; }}
  h1 {{ font-size: 20px; color: #16a34a; margin-bottom: 12px; }}
  p {{ color: #6b7280; line-height: 1.5; }}
</style>
</head>
<body>
  <div class="card">
    <h1>Authorization Successful</h1>
    <p>Your Shopline store has been connected. This window will close automatically.</p>
  </div>
  <script>
    try {{
      if (window.opener) {{
        window.opener.postMessage({{
          type: 'shopline-oauth-success',
          zendesk_subdomain: '{safe_subdomain}'
        }}, '*');
      }}
    }} catch (e) {{
      console.error('Failed to send postMessage:', e);
    }}
    setTimeout(function() {{ window.close(); }}, 1500);
  </script>
</body>
</html>"""
    return HTMLResponse(content=html)


# ---------------------------------------------------------------------------
# GET /oauth/shopline/start — render handle input form
# ---------------------------------------------------------------------------


@router.get("/start")
async def oauth_start_form(zendesk_subdomain: str | None = None):
    """Render an HTML page with a form to enter the Shopline store handle.

    The zendesk_subdomain query param is required and carried through the flow.
    """
    if not zendesk_subdomain:
        raise HTTPException(
            status_code=400,
            detail="Invalid or missing zendesk_subdomain",
        )

    try:
        validated_subdomain = validate_zendesk_subdomain(zendesk_subdomain)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid or missing zendesk_subdomain",
        )

    # Rate limit check.
    if not _check_rate_limit(validated_subdomain):
        raise HTTPException(
            status_code=429,
            detail="Too many requests. Try again later.",
        )

    safe_subdomain = escape(validated_subdomain)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>Connect Shopline Store</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif;
         display: flex; align-items: center; justify-content: center;
         min-height: 100vh; margin: 0; background: #f9fafb; }}
  .card {{ background: #fff; border-radius: 12px; padding: 32px;
           box-shadow: 0 1px 3px rgba(0,0,0,.1); max-width: 420px;
           width: 100%; }}
  h1 {{ font-size: 20px; margin-bottom: 8px; }}
  p {{ color: #6b7280; font-size: 14px; margin-bottom: 20px; }}
  label {{ display: block; font-size: 14px; font-weight: 500;
           margin-bottom: 6px; }}
  input[type="text"] {{ width: 100%; padding: 10px 12px; border: 1px solid #d1d5db;
                        border-radius: 6px; font-size: 14px; box-sizing: border-box; }}
  button {{ width: 100%; padding: 10px; margin-top: 16px; background: #2563eb;
            color: #fff; border: none; border-radius: 6px; font-size: 14px;
            cursor: pointer; }}
  button:hover {{ background: #1d4ed8; }}
  .hint {{ font-size: 12px; color: #9ca3af; margin-top: 4px; }}
</style>
</head>
<body>
  <div class="card">
    <h1>Connect Shopline Store</h1>
    <p>Enter your Shopline store handle to authorize access.</p>
    <form method="POST" action="/oauth/shopline/start">
      <input type="hidden" name="zendesk_subdomain" value="{safe_subdomain}" />
      <label for="handle">Store Handle</label>
      <input type="text" id="handle" name="handle"
             placeholder="e.g. mystore" required
             pattern="[a-zA-Z0-9_-]{{1,64}}" maxlength="64" />
      <p class="hint">The subdomain part of your-store.myshopline.com</p>
      <button type="submit">Authorize</button>
    </form>
  </div>
</body>
</html>"""
    return HTMLResponse(content=html)


# ---------------------------------------------------------------------------
# POST /oauth/shopline/start — validate, generate state, redirect
# ---------------------------------------------------------------------------


@router.post("/start")
async def oauth_start_submit(
    handle: str = Form(...),
    zendesk_subdomain: str = Form(...),
):
    """Validate inputs, generate OAuth state, and redirect to Shopline OAuth."""
    # Validate inputs.
    try:
        validated_subdomain = validate_zendesk_subdomain(zendesk_subdomain)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid or missing zendesk_subdomain",
        )

    try:
        validated_handle = validate_handle(handle)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid store handle format",
        )

    # Rate limit check.
    if not _check_rate_limit(validated_subdomain):
        raise HTTPException(
            status_code=429,
            detail="Too many requests. Try again later.",
        )

    # Generate OAuth state for CSRF protection.
    state = generate_state(
        zendesk_subdomain=validated_subdomain,
        handle=validated_handle,
    )

    # Build Shopline authorization URL.
    app_key = _env("SHOPLINE_ZD_APP_KEY")
    redirect_uri = _env("SHOPLINE_ZD_OAUTH_REDIRECT_URI")
    scopes = _env("SHOPLINE_ZD_OAUTH_SCOPES") or "read_customers,read_orders"

    auth_url = build_shopline_auth_url(
        handle=validated_handle,
        app_key=app_key,
        redirect_uri=redirect_uri,
        scopes=scopes,
        state=state,
    )

    logger.info(
        "OAuth flow started: handle=%s zendesk_subdomain=%s",
        validated_handle,
        validated_subdomain,
    )

    return RedirectResponse(url=auth_url, status_code=302)


# ---------------------------------------------------------------------------
# GET /oauth/shopline/callback — handle Shopline OAuth callback
# ---------------------------------------------------------------------------


@router.get("/callback")
@router.get("/callback/")
async def oauth_callback(request: Request):
    """Handle the Shopline OAuth callback.

    Verifies HMAC signature and OAuth state, exchanges the authorization code
    for an access token, persists the store and binding, then renders an HTML
    page that notifies the opener window and closes the popup.
    """
    params = dict(request.query_params)
    handle = params.get("handle", "")
    code = params.get("code", "")
    # State is passed via Shopline's customField parameter (not in redirectUri)
    state = params.get("customField", "") or params.get("state", "")

    # --- HMAC signature verification ---
    app_secret = _env("SHOPLINE_ZD_APP_SECRET")
    if not shopline_auth.verify_hmac(params, app_secret):
        logger.warning(
            "OAuth callback HMAC failure: handle=%s zendesk_subdomain=%s "
            "error_type=hmac_failure timestamp=%s",
            handle,
            "",  # zendesk_subdomain not yet available at this stage
            datetime.now(timezone.utc).isoformat(),
        )
        return _render_error_html(
            "Security Error",
            "Request signature verification failed. Please try again.",
        )

    # --- Look up state record to get zendesk_subdomain BEFORE consuming it ---
    if not state or not handle:
        logger.warning(
            "OAuth callback missing params: handle=%s zendesk_subdomain=%s "
            "state_present=%s error_type=state_failure timestamp=%s",
            handle,
            "",  # zendesk_subdomain not yet available
            bool(state),
            datetime.now(timezone.utc).isoformat(),
        )
        return _render_error_html(
            "Invalid Request",
            "Missing required parameters. Please try the authorization again.",
        )

    # Retrieve the state record to extract zendesk_subdomain.
    state_record = oauth_state_repo.get_state(state)
    zendesk_subdomain = state_record["zendesk_subdomain"] if state_record else ""

    # --- OAuth state verification (also deletes the record) ---
    if not verify_state(state, handle):
        logger.warning(
            "OAuth callback state failure: handle=%s zendesk_subdomain=%s "
            "error_type=state_failure timestamp=%s",
            handle,
            zendesk_subdomain,
            datetime.now(timezone.utc).isoformat(),
        )
        return _render_error_html(
            "Authorization Expired",
            "The authorization session has expired or is invalid. "
            "This may indicate a security issue. Please try again.",
        )

    if not code:
        logger.warning(
            "OAuth callback missing code: handle=%s zendesk_subdomain=%s "
            "error_type=token_exchange_error timestamp=%s",
            handle,
            zendesk_subdomain,
            datetime.now(timezone.utc).isoformat(),
        )
        return _render_error_html(
            "Authorization Failed",
            "No authorization code received from Shopline. Please try again.",
        )

    # Build retry URL for error pages.
    retry_url = f"/oauth/shopline/start?zendesk_subdomain={zendesk_subdomain}"

    # --- Exchange code for token ---
    try:
        access_token, expires_at, scopes = await shopline_auth.exchange_code_for_token(
            handle, code
        )
    except httpx.HTTPStatusError as exc:
        logger.error(
            "Token exchange HTTP error: handle=%s zendesk_subdomain=%s "
            "status=%s error_type=token_exchange_error timestamp=%s",
            handle,
            zendesk_subdomain,
            exc.response.status_code,
            datetime.now(timezone.utc).isoformat(),
        )
        return _render_error_html(
            "Token Exchange Failed",
            f"Failed to obtain access token from Shopline (HTTP {exc.response.status_code}). "
            "Please try again.",
            retry_url=retry_url,
        )
    except httpx.TimeoutException:
        logger.error(
            "Token exchange timeout: handle=%s zendesk_subdomain=%s "
            "error_type=token_exchange_error timestamp=%s",
            handle,
            zendesk_subdomain,
            datetime.now(timezone.utc).isoformat(),
        )
        return _render_error_html(
            "Request Timeout",
            "The connection to Shopline timed out. Please try again.",
            retry_url=retry_url,
        )
    except RuntimeError as exc:
        logger.error(
            "Token exchange error: handle=%s zendesk_subdomain=%s "
            "detail=%s error_type=token_exchange_error timestamp=%s",
            handle,
            zendesk_subdomain,
            str(exc),
            datetime.now(timezone.utc).isoformat(),
        )
        return _render_error_html(
            "Authorization Error",
            str(exc),
            retry_url=retry_url,
        )

    # --- Persist store and binding ---
    try:
        store = store_repo.upsert_store(
            handle=handle,
            access_token=access_token,
            expires_at=expires_at,
            scopes=scopes,
        )
        logger.info("Store upserted: handle=%s", handle)

        # Generate a simple API key for the binding (reuse handle as identifier).
        binding_repo.upsert_binding(
            store_id=str(store["id"]),
            zendesk_subdomain=zendesk_subdomain,
            api_key=handle,  # Use handle as the API key for OAuth-created bindings.
        )
        logger.info(
            "Binding upserted: handle=%s zendesk_subdomain=%s",
            handle,
            zendesk_subdomain,
        )
    except Exception as exc:
        logger.error(
            "Database error during OAuth callback: handle=%s "
            "zendesk_subdomain=%s error=%s error_type=token_exchange_error "
            "timestamp=%s",
            handle,
            zendesk_subdomain,
            str(exc),
            datetime.now(timezone.utc).isoformat(),
        )
        return _render_error_html(
            "Storage Error",
            "Failed to save the authorization. Please try again.",
            retry_url=retry_url,
        )

    # --- Success: notify opener and close popup ---
    return _render_success_html(zendesk_subdomain)


# ---------------------------------------------------------------------------
# POST /oauth/shopline/disconnect — remove binding
# ---------------------------------------------------------------------------


@router.post("/disconnect")
async def oauth_disconnect(request: Request):
    """Remove the binding for a Zendesk subdomain (keep the store record).

    Accepts JSON body: { "zendesk_subdomain": "..." }
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    zendesk_subdomain = body.get("zendesk_subdomain", "")

    try:
        validated_subdomain = validate_zendesk_subdomain(zendesk_subdomain)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid or missing zendesk_subdomain",
        )

    deleted = binding_repo.delete_binding_by_subdomain(validated_subdomain)

    if deleted:
        logger.info("Binding disconnected: zendesk_subdomain=%s", validated_subdomain)
    else:
        logger.info(
            "Disconnect: no binding found for zendesk_subdomain=%s",
            validated_subdomain,
        )

    return JSONResponse(
        content={
            "success": True,
            "zendesk_subdomain": validated_subdomain,
        }
    )
