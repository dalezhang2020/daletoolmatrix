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


# ---------------------------------------------------------------------------
# GET /binding — Current binding status
# ---------------------------------------------------------------------------


@router.get("/binding")
async def get_binding(request: Request) -> BindingResponse:
    """Return the current store-Zendesk binding status.

    The HMAC-signed query params must include ``handle``.  The response
    never exposes the API key — ``api_key`` is always ``None``.
    """
    params = dict(request.query_params)
    handle = params.get("handle", "")

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

    HMAC verification uses query params; the Zendesk subdomain comes from
    the JSON request body.  On success the response includes the newly
    generated API key (shown to the merchant once).
    """
    params = dict(request.query_params)
    handle = params.get("handle", "")

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
