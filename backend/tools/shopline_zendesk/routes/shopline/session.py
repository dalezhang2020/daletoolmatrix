"""Shopline session verification route: GET /session/verify."""

from __future__ import annotations

import logging
import os

from fastapi import APIRouter, Request

from backend.tools.shopline_zendesk.services import shopline_auth

logger = logging.getLogger(__name__)

router = APIRouter()


def _env(key: str) -> str:
    """Return an environment variable or empty string."""
    return os.environ.get(key, "")


# ---------------------------------------------------------------------------
# GET /session/verify — Frontend auth guard
# ---------------------------------------------------------------------------


@router.get("/session/verify")
async def session_verify(request: Request):
    """Verify the Shopline signed URL for the frontend session guard.

    The Shopline Frontend calls this endpoint with the full set of signed
    query params it received from the Shopline admin iframe.  If the HMAC
    signature is valid, the response includes the store handle and
    ``valid: true``.  On any failure the response is ``{valid: false}``
    (HTTP 200, **not** 401) so the frontend can render an error page
    instead of receiving an opaque error status.
    """
    params = dict(request.query_params)
    handle = params.get("handle", "")

    if not shopline_auth.verify_hmac(params, _env("SHOPLINE_ZD_APP_SECRET")):
        logger.warning("Session HMAC verification failed for handle=%s", handle)
        return {"valid": False}

    return {"handle": handle, "valid": True}
