"""Zendesk customer lookup route: GET /customer/lookup."""

from __future__ import annotations

import logging

import httpx
from fastapi import APIRouter, Header, HTTPException, Query

from backend.tools.shopline_zendesk.services import binding_service, customer_service
from backend.tools.shopline_zendesk.services.binding_service import (
    BindingNotFoundError,
    InvalidApiKeyError,
    StoreNotFoundError,
)
from backend.tools.shopline_zendesk.services.customer_service import (
    CustomerLookupResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/customer/lookup")
async def customer_lookup(
    x_api_key: str | None = Header(None, alias="X-API-Key"),
    x_zendesk_subdomain: str | None = Header(None, alias="X-Zendesk-Subdomain"),
    email: str | None = Query(None),
    phone: str | None = Query(None),
) -> CustomerLookupResponse:
    """Look up a Shopline customer by email or phone.

    Authentication is via the ``X-API-Key`` and ``X-Zendesk-Subdomain``
    headers.  The binding service resolves the Shopline store from the
    subdomain and verifies the API key.

    At least one of ``email`` or ``phone`` query params must be provided.
    """
    # -- Validate required headers -------------------------------------------
    if not x_api_key or not x_zendesk_subdomain:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # -- Validate query params -----------------------------------------------
    if not email and not phone:
        raise HTTPException(
            status_code=422,
            detail="At least one of 'email' or 'phone' query params is required",
        )

    # -- Resolve store from binding ------------------------------------------
    try:
        store = binding_service.resolve_store_from_subdomain(
            zendesk_subdomain=x_zendesk_subdomain,
            api_key=x_api_key,
        )
    except InvalidApiKeyError:
        raise HTTPException(status_code=401, detail="Unauthorized")
    except BindingNotFoundError:
        raise HTTPException(status_code=404, detail="Store not bound")
    except StoreNotFoundError:
        raise HTTPException(status_code=404, detail="Store not found")

    # -- Query Shopline Customers API ----------------------------------------
    try:
        result = await customer_service.lookup_customer(
            handle=store["handle"],
            access_token=store["access_token"],
            expires_at=store["expires_at"],
            email=email,
            phone=phone,
        )
    except httpx.HTTPStatusError as exc:
        logger.error(
            "Shopline API error for subdomain=%s: %s",
            x_zendesk_subdomain,
            exc,
        )
        raise HTTPException(
            status_code=502,
            detail={
                "detail": "Upstream error",
                "upstream_status": exc.response.status_code,
                "upstream_message": str(exc),
            },
        )
    except httpx.TimeoutException:
        logger.error(
            "Shopline API timeout for subdomain=%s",
            x_zendesk_subdomain,
        )
        raise HTTPException(status_code=502, detail="Upstream timeout")

    return result
