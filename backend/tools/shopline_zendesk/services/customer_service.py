"""Shopline Customers API client and response mapping."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx
from pydantic import BaseModel

from backend.tools.shopline_zendesk.db import store_repo
from backend.tools.shopline_zendesk.services import shopline_auth

logger = logging.getLogger(__name__)

_TIMEOUT = 15.0  # seconds
_API_VERSION = "v20250601"


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class CustomerInfo(BaseModel):
    """Zendesk-facing customer data model."""

    id: str
    name: str
    email: str | None
    phone: str | None
    total_order_count: int
    total_spend: str  # Decimal as string to avoid float precision issues
    currency: str
    registered_at: str | None


class CustomerLookupResponse(BaseModel):
    """Response wrapper for customer lookup."""

    customers: list[CustomerInfo]
    total: int


class CustomerListResponse(BaseModel):
    """Response wrapper for paginated customer list."""

    customers: list[CustomerInfo]
    total: int
    page: int
    limit: int


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _map_customer(raw: dict[str, Any]) -> CustomerInfo:
    """Map a single Shopline API customer object to a ``CustomerInfo``.

    Applies safe defaults for missing or ``None`` fields so the caller
    never encounters missing keys.
    """
    first_name = raw.get("first_name") or ""
    last_name = raw.get("last_name") or ""
    # Shopline may also provide a "nickname" field; fall back to it if
    # first/last are both empty.
    name = f"{first_name} {last_name}".strip()
    if not name:
        name = raw.get("nickname") or "Unknown"

    return CustomerInfo(
        id=str(raw.get("id", "")),
        name=name,
        email=raw.get("email") or None,
        phone=raw.get("phone") or None,
        total_order_count=int(raw.get("orders_count") or 0),
        total_spend=str(raw.get("total_spent") or "0.00"),
        currency=raw.get("currency") or "USD",
        registered_at=raw.get("created_at") or None,
    )


# ---------------------------------------------------------------------------
# Token freshness check
# ---------------------------------------------------------------------------


async def _ensure_fresh_token(
    handle: str,
    access_token: str,
    expires_at: datetime,
) -> str:
    """Return a valid access token, refreshing if expired.

    If the token is still valid, returns it as-is.  Otherwise calls the
    Shopline refresh API and persists the new token via ``store_repo``.

    Raises:
        httpx.HTTPStatusError: If the refresh request fails (e.g. app
            uninstalled).
    """
    now = datetime.now(tz=timezone.utc)
    if expires_at.tzinfo is None:
        # Treat naive datetimes as UTC for safety.
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    if now < expires_at:
        return access_token

    logger.info("Token expired for handle=%s, refreshing…", handle)
    new_token, new_expires_at = await shopline_auth.refresh_token(handle, access_token)
    store_repo.update_token(handle, new_token, new_expires_at)
    return new_token


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def lookup_customer(
    handle: str,
    access_token: str,
    expires_at: datetime,
    email: str | None = None,
    phone: str | None = None,
) -> CustomerLookupResponse:
    """Query the Shopline Customers Search API by email or phone.

    Uses the ``/customers/v2/search.json`` endpoint with the
    ``query_param`` parameter for fuzzy matching on email or phone.

    Args:
        handle: Shopline store handle (e.g. ``"mystore"``).
        access_token: Current Shopline access token.
        expires_at: Token expiry timestamp.
        email: Customer email to search for (optional).
        phone: Customer phone to search for (optional).

    Returns:
        A ``CustomerLookupResponse`` containing matched customers and
        the total count.

    Raises:
        httpx.HTTPStatusError: On non-2xx responses from Shopline.
        httpx.TimeoutException: If the request exceeds the timeout.
    """
    token = await _ensure_fresh_token(handle, access_token, expires_at)

    url = (
        f"https://{handle}.myshopline.com/admin/openapi/"
        f"{_API_VERSION}/customers/v2/search.json"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }

    # Build query params — use query_param for fuzzy search by email/phone.
    params: dict[str, str] = {"limit": "50"}
    if email:
        params["query_param"] = email
    elif phone:
        params["query_param"] = phone

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.get(url, params=params, headers=headers)
        resp.raise_for_status()

    data = resp.json()
    raw_customers: list[dict] = data.get("customers", [])

    customers = [_map_customer(c) for c in raw_customers]
    return CustomerLookupResponse(customers=customers, total=len(customers))


async def list_customers(
    handle: str,
    access_token: str,
    expires_at: datetime,
    page: int = 1,
    limit: int = 20,
    search: str | None = None,
) -> CustomerListResponse:
    """Return a paginated list of customers from the Shopline Open API.

    Uses the ``/customers/v2/search.json`` endpoint.  When *search* is
    provided it is forwarded as ``query_param`` for fuzzy matching on
    email/phone/name.

    Args:
        handle: Shopline store handle (e.g. ``"mystore"``).
        access_token: Current Shopline access token.
        expires_at: Token expiry timestamp.
        page: 1-based page number (used for frontend display only;
            Shopline API uses offset-style via ``limit``).
        limit: Number of customers per page (1–100).
        search: Optional search query forwarded to Shopline.

    Returns:
        A ``CustomerListResponse`` with the customer list and pagination
        metadata.

    Raises:
        httpx.HTTPStatusError: On non-2xx responses from Shopline.
        httpx.TimeoutException: If the request exceeds the timeout.
    """
    token = await _ensure_fresh_token(handle, access_token, expires_at)

    url = (
        f"https://{handle}.myshopline.com/admin/openapi/"
        f"{_API_VERSION}/customers/v2/search.json"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }

    params: dict[str, str] = {"limit": str(limit)}
    if search:
        params["query_param"] = search

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.get(url, params=params, headers=headers)
        resp.raise_for_status()

    data = resp.json()
    raw_customers: list[dict] = data.get("customers", [])

    customers = [_map_customer(c) for c in raw_customers]
    return CustomerListResponse(
        customers=customers,
        total=len(customers),
        page=page,
        limit=limit,
    )
