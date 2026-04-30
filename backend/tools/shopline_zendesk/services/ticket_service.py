"""Zendesk Search API client for ticket lookup by customer email."""

from __future__ import annotations

import base64
import logging
from typing import Any

import httpx
from pydantic import BaseModel

logger = logging.getLogger(__name__)

_TIMEOUT = 15.0  # seconds


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class TicketInfo(BaseModel):
    """Zendesk ticket data model."""

    id: int
    subject: str
    status: str
    priority: str | None
    created_at: str
    updated_at: str
    url: str  # https://{subdomain}.zendesk.com/agent/tickets/{id}


class TicketListResponse(BaseModel):
    """Response wrapper for Zendesk ticket search.

    Always returned (never raises).  On failure the ``error`` field
    contains a human-readable message and ``tickets`` is empty.
    """

    tickets: list[TicketInfo]
    total: int
    error: str | None  # Non-null if Zendesk API failed (partial failure)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_auth_header(admin_email: str, api_token: str) -> str:
    """Construct the HTTP Basic Auth header for Zendesk API.

    Uses the ``{email}/token:{api_token}`` format required by Zendesk.
    """
    credentials = f"{admin_email}/token:{api_token}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return f"Basic {encoded}"


def _build_search_query(customer_email: str) -> str:
    """Construct the Zendesk search query for tickets by requester email."""
    return f"type:ticket requester:{customer_email}"


def _map_ticket(raw: dict[str, Any], subdomain: str) -> TicketInfo:
    """Map a single Zendesk search result to a ``TicketInfo``.

    Applies safe defaults for missing or ``None`` fields.
    """
    ticket_id = int(raw.get("id") or 0)
    return TicketInfo(
        id=ticket_id,
        subject=str(raw.get("subject") or ""),
        status=str(raw.get("status") or "unknown"),
        priority=raw.get("priority") or None,
        created_at=str(raw.get("created_at") or ""),
        updated_at=str(raw.get("updated_at") or ""),
        url=f"https://{subdomain}.zendesk.com/agent/tickets/{ticket_id}",
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def search_tickets(
    subdomain: str,
    admin_email: str,
    api_token: str,
    customer_email: str,
) -> TicketListResponse:
    """Search Zendesk for tickets filed by *customer_email*.

    This function **never raises**.  All errors are captured and returned
    via the ``error`` field of ``TicketListResponse`` so that the caller
    can render customer/order data even when the ticket fetch fails
    (partial failure isolation).

    Args:
        subdomain: Zendesk subdomain (e.g. ``"mycompany"``).
        admin_email: Zendesk admin email for Basic auth.
        api_token: Zendesk API token for Basic auth.
        customer_email: The customer's email to search tickets for.

    Returns:
        A ``TicketListResponse`` — always.  On success ``error`` is
        ``None``; on failure ``error`` describes what went wrong and
        ``tickets`` is an empty list.
    """
    try:
        url = f"https://{subdomain}.zendesk.com/api/v2/search.json"
        headers = {
            "Authorization": _build_auth_header(admin_email, api_token),
            "Content-Type": "application/json",
        }
        params = {"query": _build_search_query(customer_email)}

        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.get(url, params=params, headers=headers)
            resp.raise_for_status()

        data = resp.json()
        raw_results: list[dict] = data.get("results", [])

        tickets = [_map_ticket(r, subdomain) for r in raw_results]
        return TicketListResponse(
            tickets=tickets,
            total=int(data.get("count", len(tickets))),
            error=None,
        )

    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if status == 401:
            msg = "Zendesk credentials invalid or expired"
        elif status == 429:
            msg = "Zendesk rate limit exceeded, try again later"
        else:
            msg = "Failed to fetch tickets"
        logger.warning(
            "Zendesk API error for subdomain=%s status=%s: %s",
            subdomain, status, msg,
        )
        return TicketListResponse(tickets=[], total=0, error=msg)

    except Exception as exc:
        logger.warning(
            "Zendesk API unexpected error for subdomain=%s: %s",
            subdomain, exc,
        )
        return TicketListResponse(
            tickets=[], total=0, error="Failed to fetch tickets",
        )
