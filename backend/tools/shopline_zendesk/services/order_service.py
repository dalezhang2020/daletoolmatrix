"""Shopline Orders API client and response mapping."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

import httpx
from pydantic import BaseModel

from backend.tools.shopline_zendesk.db import store_repo
from backend.tools.shopline_zendesk.services.customer_service import (
    _ensure_fresh_token,
)

logger = logging.getLogger(__name__)

_TIMEOUT = 15.0  # seconds
_API_VERSION = "v20260901"


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class OrderLineItem(BaseModel):
    """A single line item within an order."""

    name: str
    sku: str | None
    quantity: int
    price: str
    total_price: str


class OrderInfo(BaseModel):
    """Shopline order data model."""

    id: str
    order_number: str
    status: str
    fulfillment_status: str | None
    financial_status: str | None
    line_items: list[OrderLineItem]
    total_price: str
    currency: str
    created_at: str
    updated_at: str


class OrderListResponse(BaseModel):
    """Response wrapper for customer order list."""

    orders: list[OrderInfo]
    next_page_info: str | None  # cursor for next page, None if last page


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# Regex to extract page_info from Shopline Link header.
# Example: <https://...?page_info=abc123&limit=50>; rel="next"
_PAGE_INFO_RE = re.compile(r'[?&]page_info=([^&>]+)')


def _extract_page_info(link_header: str | None) -> str | None:
    """Parse the ``page_info`` cursor from a Shopline ``Link`` header.

    The Shopline API returns cursor-based pagination via the ``Link``
    response header.  This function extracts the ``page_info`` value
    from the URL in the header.

    Returns ``None`` if the header is missing, empty, or does not
    contain a ``page_info`` parameter.
    """
    if not link_header:
        return None
    match = _PAGE_INFO_RE.search(link_header)
    if match:
        return match.group(1)
    return None


def _map_line_item(raw: dict[str, Any]) -> OrderLineItem:
    """Map a single raw line item dict to an ``OrderLineItem``."""
    return OrderLineItem(
        name=str(raw.get("name") or "Unknown item"),
        sku=raw.get("sku") or None,
        quantity=int(raw.get("quantity") or 0),
        price=str(raw.get("price") or "0.00"),
        total_price=str(raw.get("total_price") or "0.00"),
    )


def _map_order(raw: dict[str, Any]) -> OrderInfo:
    """Map a single Shopline API order object to an ``OrderInfo``.

    Applies safe defaults for missing or ``None`` fields so the caller
    never encounters missing keys or unexpected exceptions.
    """
    raw_items: list[dict] = raw.get("line_items") or []
    line_items = [_map_line_item(item) for item in raw_items]

    return OrderInfo(
        id=str(raw.get("id") or ""),
        order_number=str(raw.get("order_number") or ""),
        status=str(raw.get("status") or "unknown"),
        fulfillment_status=raw.get("fulfillment_status") or None,
        financial_status=raw.get("financial_status") or None,
        line_items=line_items,
        total_price=str(raw.get("total_price") or "0.00"),
        currency=str(raw.get("currency") or "USD"),
        created_at=str(raw.get("created_at") or ""),
        updated_at=str(raw.get("updated_at") or ""),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def get_customer_orders(
    handle: str,
    access_token: str,
    expires_at: datetime,
    customer_id: str,
    page_info: str | None = None,
    limit: int = 50,
) -> OrderListResponse:
    """Fetch orders for a specific customer from the Shopline Open API.

    Uses the ``/customers/{id}/orders.json`` endpoint with cursor-based
    pagination via the ``page_info`` parameter.

    Args:
        handle: Shopline store handle (e.g. ``"mystore"``).
        access_token: Current Shopline access token.
        expires_at: Token expiry timestamp.
        customer_id: Shopline customer ID.
        page_info: Cursor for the next page (from a previous response).
        limit: Number of orders per page (default 50, max 100).

    Returns:
        An ``OrderListResponse`` with the order list and the next page
        cursor (``None`` if this is the last page).

    Raises:
        httpx.HTTPStatusError: On non-2xx responses from Shopline.
        httpx.TimeoutException: If the request exceeds the timeout.
    """
    token = await _ensure_fresh_token(handle, access_token, expires_at)

    url = (
        f"https://{handle}.myshopline.com/admin/openapi/"
        f"{_API_VERSION}/customers/{customer_id}/orders.json"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }

    params: dict[str, str] = {"limit": str(limit)}
    if page_info:
        params["page_info"] = page_info

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.get(url, params=params, headers=headers)
        resp.raise_for_status()

    data = resp.json()
    raw_orders: list[dict] = data.get("orders", [])

    orders = [_map_order(o) for o in raw_orders]
    next_cursor = _extract_page_info(resp.headers.get("link"))

    return OrderListResponse(orders=orders, next_page_info=next_cursor)
