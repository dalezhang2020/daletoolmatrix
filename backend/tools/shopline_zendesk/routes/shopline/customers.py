"""Shopline customer routes: list, orders, tickets."""

from __future__ import annotations

import logging
import os

import httpx
from fastapi import APIRouter, HTTPException, Query, Request

from backend.tools.shopline_zendesk.db import binding_repo, store_repo
from backend.tools.shopline_zendesk.services import shopline_auth
from backend.tools.shopline_zendesk.services.customer_service import (
    CustomerListResponse,
    _ensure_fresh_token,
    list_customers,
)
from backend.tools.shopline_zendesk.services.order_service import (
    OrderListResponse,
    get_customer_orders,
)
from backend.tools.shopline_zendesk.services.ticket_service import (
    TicketListResponse,
    search_tickets,
)

logger = logging.getLogger(__name__)

router = APIRouter()


def _env(key: str) -> str:
    """Return an environment variable or empty string."""
    return os.environ.get(key, "")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _get_handle_or_raise(request: Request) -> str:
    """Extract and validate the ``handle`` query param.

    If a ``sign`` param is present the full HMAC is verified; otherwise
    the iframe context is trusted (same pattern as binding.py).
    """
    params = dict(request.query_params)
    handle = params.get("handle", "")

    if not handle:
        raise HTTPException(status_code=400, detail="Missing handle parameter")

    if params.get("sign"):
        if not shopline_auth.verify_hmac(params, _env("SHOPLINE_ZD_APP_SECRET")):
            logger.warning("Customers HMAC verification failed for handle=%s", handle)
            raise HTTPException(status_code=401, detail="Invalid signature")

    return handle


def _lookup_store(handle: str) -> dict:
    """Look up a store by handle, raising 404 if not found."""
    store = store_repo.get_store_by_handle(handle)
    if store is None:
        raise HTTPException(status_code=404, detail="Store not found")
    return store


# ---------------------------------------------------------------------------
# GET /customers — Paginated customer list
# ---------------------------------------------------------------------------


@router.get("/customers", response_model=CustomerListResponse)
async def get_customers(
    request: Request,
    handle: str = Query(..., description="Shopline store handle"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, description="Items per page"),
    search: str | None = Query(None, description="Search query"),
) -> CustomerListResponse:
    """Return a paginated list of customers for the store.

    Validates ``limit`` (1–100) and ``search`` length (≤200 chars).
    """
    # --- Input validation ---
    if limit < 1 or limit > 100:
        raise HTTPException(
            status_code=422,
            detail="limit must be between 1 and 100",
        )
    if search is not None and len(search) > 200:
        raise HTTPException(
            status_code=422,
            detail="Search query too long (max 200)",
        )

    # --- Auth ---
    _get_handle_or_raise(request)

    # --- Store lookup + token refresh ---
    store = _lookup_store(handle)
    token = await _ensure_fresh_token(
        handle, store["access_token"], store["expires_at"],
    )

    # --- Shopline API call ---
    try:
        return await list_customers(
            handle=handle,
            access_token=token,
            expires_at=store["expires_at"],
            page=page,
            limit=limit,
            search=search,
        )
    except httpx.HTTPStatusError as exc:
        logger.warning(
            "Shopline customers API error handle=%s status=%s",
            handle, exc.response.status_code,
        )
        raise HTTPException(
            status_code=502,
            detail="Upstream error",
            headers={"X-Upstream-Status": str(exc.response.status_code)},
        )
    except httpx.TimeoutException:
        logger.warning("Shopline customers API timeout handle=%s", handle)
        raise HTTPException(status_code=502, detail="Upstream timeout")


# ---------------------------------------------------------------------------
# GET /customers/{customer_id}/orders — Customer order list
# ---------------------------------------------------------------------------


@router.get(
    "/customers/{customer_id}/orders",
    response_model=OrderListResponse,
)
async def get_orders(
    customer_id: str,
    request: Request,
    handle: str = Query(..., description="Shopline store handle"),
    page_info: str | None = Query(None, description="Cursor for next page"),
) -> OrderListResponse:
    """Return orders for a specific customer."""
    _get_handle_or_raise(request)

    store = _lookup_store(handle)
    token = await _ensure_fresh_token(
        handle, store["access_token"], store["expires_at"],
    )

    try:
        return await get_customer_orders(
            handle=handle,
            access_token=token,
            expires_at=store["expires_at"],
            customer_id=customer_id,
            page_info=page_info,
        )
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        logger.warning(
            "Shopline orders API error handle=%s customer=%s status=%s",
            handle, customer_id, status,
        )
        raise HTTPException(
            status_code=502,
            detail="Upstream error",
            headers={"X-Upstream-Status": str(status)},
        )
    except httpx.TimeoutException:
        logger.warning(
            "Shopline orders API timeout handle=%s customer=%s",
            handle, customer_id,
        )
        raise HTTPException(status_code=502, detail="Upstream timeout")


# ---------------------------------------------------------------------------
# GET /customers/{customer_id}/tickets — Zendesk tickets for customer
# ---------------------------------------------------------------------------


@router.get(
    "/customers/{customer_id}/tickets",
    response_model=TicketListResponse,
)
async def get_tickets(
    customer_id: str,
    request: Request,
    handle: str = Query(..., description="Shopline store handle"),
    email: str | None = Query(None, description="Customer email for ticket lookup"),
) -> TicketListResponse:
    """Return Zendesk tickets associated with the customer's email.

    Always returns 200 — errors are communicated via the ``error`` field
    in the response body (partial failure isolation).

    The ``email`` query param lets the frontend pass the customer email
    directly, avoiding an extra API call.  If ``email`` is not provided,
    the endpoint returns an error asking the caller to supply it.
    """
    _get_handle_or_raise(request)

    # --- Email is required ---
    if not email:
        return TicketListResponse(
            tickets=[],
            total=0,
            error="Customer email is required for ticket lookup. Pass email as a query parameter.",
        )

    # --- Zendesk binding lookup ---
    binding = binding_repo.get_binding_by_handle(handle)
    if binding is None:
        return TicketListResponse(
            tickets=[],
            total=0,
            error="No Zendesk binding configured",
        )

    subdomain = binding.get("zendesk_subdomain")
    access_token = binding.get("zendesk_access_token")

    # Prefer OAuth access_token; fall back to legacy credentials never
    if not access_token:
        return TicketListResponse(
            tickets=[],
            total=0,
            error="Zendesk not connected. Please connect Zendesk via OAuth.",
        )

    # --- Zendesk ticket search (never raises) ---
    return await search_tickets(
        subdomain=subdomain,
        access_token=access_token,
        customer_email=email,
    )
