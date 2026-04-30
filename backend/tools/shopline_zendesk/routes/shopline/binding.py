"""Shopline store-Zendesk binding routes: GET, PUT /binding, POST /binding/verify-zendesk."""

from __future__ import annotations

import base64
import logging
import os

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from backend.tools.shopline_zendesk.services import binding_service, shopline_auth
from backend.tools.shopline_zendesk.services.binding_service import StoreNotFoundError

logger = logging.getLogger(__name__)

router = APIRouter()

_ZENDESK_VERIFY_TIMEOUT = 10.0  # seconds


def _env(key: str) -> str:
    """Return an environment variable or empty string."""
    return os.environ.get(key, "")


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------


class BindingRequest(BaseModel):
    zendesk_subdomain: str = Field(
        ...,
        pattern=r"^[a-z0-9-]+$",
        min_length=1,
        max_length=63,
    )
    zendesk_admin_email: str | None = Field(
        default=None,
        max_length=254,
    )
    zendesk_api_token: str | None = Field(
        default=None,
        max_length=256,
    )


class VerifyZendeskRequest(BaseModel):
    zendesk_subdomain: str = Field(
        ...,
        pattern=r"^[a-z0-9-]+$",
        min_length=1,
        max_length=63,
    )
    zendesk_admin_email: str = Field(
        ...,
        min_length=1,
        max_length=254,
    )
    zendesk_api_token: str = Field(
        ...,
        min_length=1,
        max_length=256,
    )


class VerifyZendeskResponse(BaseModel):
    valid: bool
    name: str | None = None
    error: str | None = None


class BindingResponse(BaseModel):
    handle: str
    zendesk_subdomain: str | None
    api_key: str | None
    has_zendesk_credentials: bool = False


# ---------------------------------------------------------------------------
# GET /binding — Current binding status
# ---------------------------------------------------------------------------


@router.get("/binding")
async def get_binding(request: Request) -> BindingResponse:
    """Return the current store-Zendesk binding status.

    Accepts either HMAC-signed params (from Shopline platform) or just
    a ``handle`` param (from the Vercel frontend loaded via /entry).
    The /entry endpoint already verified the HMAC, so the frontend
    running inside the Shopline iframe is trusted.
    """
    params = dict(request.query_params)
    handle = params.get("handle", "")

    if not handle:
        raise HTTPException(status_code=400, detail="Missing handle parameter")

    # If HMAC params are present, verify them; otherwise trust the iframe context
    if params.get("sign"):
        if not shopline_auth.verify_hmac(params, _env("SHOPLINE_ZD_APP_SECRET")):
            logger.warning("Binding GET HMAC verification failed for handle=%s", handle)
            raise HTTPException(status_code=401, detail="Invalid signature")

    result = binding_service.get_binding_status(handle)
    return BindingResponse(**result)


# ---------------------------------------------------------------------------
# PUT /binding — Create or update binding
# ---------------------------------------------------------------------------


@router.put("/binding")
async def put_binding(request: Request, body: BindingRequest) -> BindingResponse:
    """Create or update the store-Zendesk binding.

    Accepts either HMAC-signed query params or just a ``handle`` param.
    The Zendesk subdomain, admin email, and API token come from the JSON body.
    """
    params = dict(request.query_params)
    handle = params.get("handle", "")

    if not handle:
        raise HTTPException(status_code=400, detail="Missing handle parameter")

    # If HMAC params are present, verify them; otherwise trust the iframe context
    if params.get("sign"):
        if not shopline_auth.verify_hmac(params, _env("SHOPLINE_ZD_APP_SECRET")):
            logger.warning("Binding PUT HMAC verification failed for handle=%s", handle)
            raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        result = binding_service.create_or_update_binding(
            handle=handle,
            zendesk_subdomain=body.zendesk_subdomain,
            zendesk_admin_email=body.zendesk_admin_email,
            zendesk_api_token=body.zendesk_api_token,
        )
    except StoreNotFoundError:
        raise HTTPException(status_code=404, detail="Store not found")

    return BindingResponse(**result)


# ---------------------------------------------------------------------------
# POST /binding/verify-zendesk — Validate Zendesk credentials
# ---------------------------------------------------------------------------


@router.post("/binding/verify-zendesk")
async def verify_zendesk(body: VerifyZendeskRequest) -> VerifyZendeskResponse:
    """Verify Zendesk admin email + API token by calling the Zendesk API.

    Calls ``GET /api/v2/users/me.json`` with Basic Auth to confirm the
    credentials are valid before the frontend saves them.
    """
    credentials = f"{body.zendesk_admin_email}/token:{body.zendesk_api_token}"
    encoded = base64.b64encode(credentials.encode()).decode()
    auth_header = f"Basic {encoded}"

    url = f"https://{body.zendesk_subdomain}.zendesk.com/api/v2/users/me.json"

    try:
        async with httpx.AsyncClient(timeout=_ZENDESK_VERIFY_TIMEOUT) as client:
            resp = await client.get(
                url,
                headers={"Authorization": auth_header},
            )

        if resp.status_code == 200:
            data = resp.json()
            user = data.get("user", {})
            name = user.get("name", "")
            return VerifyZendeskResponse(valid=True, name=name)

        if resp.status_code == 401:
            return VerifyZendeskResponse(
                valid=False,
                error="Invalid credentials. Check your email and API token.",
            )

        return VerifyZendeskResponse(
            valid=False,
            error=f"Zendesk returned status {resp.status_code}",
        )

    except httpx.TimeoutException:
        return VerifyZendeskResponse(
            valid=False,
            error="Connection to Zendesk timed out. Check your subdomain.",
        )
    except httpx.ConnectError:
        return VerifyZendeskResponse(
            valid=False,
            error="Could not connect to Zendesk. Check your subdomain.",
        )
    except Exception as exc:
        logger.warning("Zendesk verify error: %s", exc)
        return VerifyZendeskResponse(
            valid=False,
            error="Failed to verify credentials",
        )
