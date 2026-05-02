"""Tenant middleware: resolves Zendesk subdomain + store handle.

Queries shopline_zendesk.bindings + shopline_zendesk.stores to inject the
selected Shopline store into request.state for downstream routers
(customers, orders, logistics, subscriptions, etc.).
"""

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
import logging

from backend.db.connection import get_connection

logger = logging.getLogger(__name__)

# Paths that require tenant resolution
_MANAGED_PREFIXES = (
    "/api/customers",
    "/api/orders",
    "/api/logistics",
    "/api/subscriptions",
    "/api/tenants",
    "/api/users",
    "/api/stripe",
)

# Paths that should be skipped entirely (exact match)
_EXACT_SKIP = frozenset([
    "/", "/health", "/docs", "/redoc", "/openapi.json", "/favicon.ico",
])

# Paths that should be skipped (prefix match)
_PREFIX_SKIP = (
    "/api/subscriptions/tiers",
    "/api/users/",
    "/api/stripe/",
    "/api/tenants/",
)


def _lookup_tenants(zendesk_subdomain: str) -> list[dict]:
    """Query all Shopline bindings linked to the Zendesk subdomain."""
    sql = """
        SELECT s.handle,
               s.access_token,
               s.token_invalid,
               b.id AS binding_id,
               s.id AS store_id
        FROM shopline_zendesk.bindings b
        JOIN shopline_zendesk.stores s ON s.id = b.store_id
        WHERE b.zendesk_subdomain = %s
        ORDER BY b.updated_at DESC, s.handle ASC
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (zendesk_subdomain,))
            rows = cur.fetchall()
    return [
        {
            "handle": row[0],
            "access_token": row[1],
            "token_invalid": row[2],
            "binding_id": str(row[3]),
            "store_id": str(row[4]),
        }
        for row in rows
    ]


class TenantMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Skip OPTIONS (CORS preflight)
        if request.method == "OPTIONS":
            return await call_next(request)

        # Only process managed API prefixes
        if not request.url.path.startswith(_MANAGED_PREFIXES):
            return await call_next(request)

        # Check skip lists
        should_skip = (
            request.url.path in _EXACT_SKIP
            or any(request.url.path.startswith(p) for p in _PREFIX_SKIP)
        )
        if should_skip:
            return await call_next(request)

        # Resolve Zendesk subdomain
        zendesk_subdomain = request.headers.get("X-Zendesk-Subdomain")
        if not zendesk_subdomain:
            logger.warning("Missing X-Zendesk-Subdomain for %s", request.url.path)
            if request.url.path.startswith("/api/"):
                return JSONResponse(
                    status_code=400,
                    content={
                        "success": False,
                        "error": "Zendesk subdomain not found in request headers.",
                        "code": "ZENDESK_SUBDOMAIN_MISSING",
                    },
                )

        requested_handle = (
            request.headers.get("X-Shopline-Handle")
            or request.query_params.get("shopline_handle")
        )

        # Look up tenant config from shopline_zendesk schema
        try:
            tenants = _lookup_tenants(zendesk_subdomain)
        except Exception as e:
            logger.error("Tenant lookup failed for %s: %s", zendesk_subdomain, e)
            if request.url.path.startswith("/api/"):
                return JSONResponse(
                    status_code=500,
                    content={
                        "success": False,
                        "error": "Failed to load tenant configuration",
                        "code": "TENANT_CONFIG_ERROR",
                    },
                )
            return await call_next(request)

        if not tenants:
            logger.warning("No binding found for subdomain: %s", zendesk_subdomain)
            if request.url.path.startswith("/api/"):
                return JSONResponse(
                    status_code=400,
                    content={
                        "success": False,
                        "error": f"No configuration found for Zendesk subdomain: {zendesk_subdomain}",
                        "code": "TENANT_CONFIG_NOT_FOUND",
                    },
                )
        elif requested_handle:
            tenant = next(
                (item for item in tenants if item["handle"] == requested_handle),
                None,
            )
            if tenant is None:
                return JSONResponse(
                    status_code=404,
                    content={
                        "success": False,
                        "error": (
                            f"Store {requested_handle} is not linked to Zendesk "
                            f"subdomain: {zendesk_subdomain}"
                        ),
                        "code": "STORE_NOT_LINKED",
                    },
                )
        elif len(tenants) == 1:
            tenant = tenants[0]
        else:
            available_handles = [item["handle"] for item in tenants]
            logger.info(
                "Multiple stores linked to %s; explicit store selection required",
                zendesk_subdomain,
            )
            return JSONResponse(
                status_code=409,
                content={
                    "success": False,
                    "error": (
                        "Multiple Shopline stores are linked to this Zendesk "
                        "account. Select a store first."
                    ),
                    "code": "STORE_SELECTION_REQUIRED",
                    "available_stores": available_handles,
                },
            )

        # Inject into request.state for downstream routers
        request.state.shopline_domain = tenant["handle"]
        request.state.shopline_access_token = tenant["access_token"]
        request.state.zendesk_subdomain = zendesk_subdomain
        request.state.tenant_handle = tenant["handle"]
        request.state.tenant_store_id = tenant["store_id"]
        request.state.binding_id = tenant["binding_id"]
        request.state.token_invalid = tenant["token_invalid"]
        logger.info(
            "Tenant resolved: %s → %s (token_invalid=%s)",
            zendesk_subdomain,
            tenant["handle"],
            tenant["token_invalid"],
        )

        return await call_next(request)
