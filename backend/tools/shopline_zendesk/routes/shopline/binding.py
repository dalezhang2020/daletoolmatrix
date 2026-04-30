"""Shopline store-Zendesk binding routes: GET /binding, PUT /binding."""

from __future__ import annotations

import logging
import os

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from backend.tools.shopline_zendesk.services import binding_service, shopline_auth
from backend.tools.shopline_zendesk.services.binding_service import StoreNotFoundError

logger = logging.getLogger(__name__)

router = APIRouter()


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
    The Zendesk subdomain comes from the JSON request body.
    Zendesk credentials are now managed via OAuth (Connect Zendesk button).
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
        )
    except StoreNotFoundError:
        raise HTTPException(status_code=404, detail="Store not found")

    return BindingResponse(**result)
