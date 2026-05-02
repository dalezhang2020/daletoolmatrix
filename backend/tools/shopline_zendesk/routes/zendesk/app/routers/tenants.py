"""Tenant configuration routes for the ZAF app.

All queries go through shopline_zendesk.stores + shopline_zendesk.bindings
via the shared psycopg2 connection layer (backend.db.connection).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.db.connection import get_connection
from backend.tools.shopline_zendesk.db import binding_repo, store_repo
from backend.tools.shopline_zendesk.services import api_key_service

logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class ShoplineConfigValidation(BaseModel):
    shopline_domain: str
    shopline_access_token: str


class TenantConfigSetup(BaseModel):
    zendesk_subdomain: str
    shopline_domain: str
    shopline_access_token: str


# ---------------------------------------------------------------------------
# GET /config/{zendesk_subdomain}
# ---------------------------------------------------------------------------

@router.get("/config/{zendesk_subdomain}")
async def get_tenant_config(zendesk_subdomain: str):
    """Return binding metadata for the Zendesk subdomain.

    This endpoint is intentionally metadata-only. The frontend learns which
    stores are linked, then selects one store handle for subsequent API calls.
    Raw Shopline access tokens are never returned here.
    """
    try:
        bindings = binding_repo.list_bindings_by_subdomain(zendesk_subdomain)
        if not bindings:
            raise HTTPException(status_code=404, detail="Tenant not found")

        stores = []
        for binding in bindings:
            store = store_repo.get_store_by_id(binding["store_id"])
            if not store:
                logger.warning(
                    "Store missing for binding store_id=%s subdomain=%s",
                    binding["store_id"],
                    zendesk_subdomain,
                )
                continue

            stores.append(
                {
                    "store_id": str(binding["store_id"]),
                    "handle": store["handle"],
                    "token_invalid": bool(store.get("token_invalid", False)),
                    "has_zendesk_credentials": bool(
                        binding.get("has_zendesk_credentials", False)
                    ),
                }
            )

        if not stores:
            raise HTTPException(status_code=404, detail="Store not found")

        default_store_handle = stores[0]["handle"] if len(stores) == 1 else None

        return {
            "success": True,
            "data": {
                "zendesk_subdomain": zendesk_subdomain,
                "store_count": len(stores),
                "multiple_stores": len(stores) > 1,
                "default_store_handle": default_store_handle,
                "shopline_domain": default_store_handle,
                "handle": default_store_handle,
                "token_invalid": stores[0]["token_invalid"] if len(stores) == 1 else None,
                "stores": stores,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error getting tenant config for %s: %s", zendesk_subdomain, e)
        return {"success": False, "error": str(e)}


# ---------------------------------------------------------------------------
# POST /validate-shopline-config
# ---------------------------------------------------------------------------

@router.post("/validate-shopline-config")
async def validate_shopline_config(config: ShoplineConfigValidation):
    """Validate Shopline credentials by calling the Shopline API."""
    try:
        url = (
            f"https://{config.shopline_domain}.myshopline.com"
            f"/admin/openapi/v20250601/merchants/shop.json"
        )
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(
                url,
                headers={
                    "Authorization": f"Bearer {config.shopline_access_token}",
                    "Content-Type": "application/json; charset=utf-8",
                },
                timeout=10.0,
            )

        if response.status_code == 200:
            return {"success": True, "data": {"valid": True, "message": "Shopline configuration is valid"}}

        logger.error("Shopline validation failed: %s - %s", response.status_code, response.text)
        return {"success": False, "error": f"Invalid configuration: {response.status_code}", "data": {"valid": False}}

    except httpx.TimeoutException:
        return {"success": False, "error": "Validation timeout - please check the domain", "data": {"valid": False}}
    except Exception as e:
        logger.error("Error validating Shopline config: %s", e)
        return {"success": False, "error": str(e), "data": {"valid": False}}


# ---------------------------------------------------------------------------
# POST /setup-config
# ---------------------------------------------------------------------------

@router.post("/setup-config")
async def setup_tenant_config(config: TenantConfigSetup):
    """Validate and save tenant configuration.

    1. Validate Shopline credentials.
    2. Upsert into shopline_zendesk.stores.
    3. Upsert into shopline_zendesk.bindings.
    """
    try:
        # Validate first
        url = (
            f"https://{config.shopline_domain}.myshopline.com"
            f"/admin/openapi/v20250601/merchants/shop.json"
        )
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(
                url,
                headers={
                    "Authorization": f"Bearer {config.shopline_access_token}",
                    "Content-Type": "application/json; charset=utf-8",
                },
                timeout=10.0,
            )

        if response.status_code != 200:
            return {
                "success": False,
                "error": f"Invalid Shopline configuration: {response.status_code}",
                "data": {"valid": False},
            }

        # Manual token entry is treated as a long-lived fallback credential.
        manual_expiry = datetime.utcnow() + timedelta(days=3650)

        # Upsert store
        store = store_repo.upsert_store(
            handle=config.shopline_domain,
            access_token=config.shopline_access_token,
            expires_at=manual_expiry,
            scopes="read_customers,read_orders",
        )

        # Upsert binding
        existing_binding = binding_repo.get_binding_by_handle(config.shopline_domain)
        api_key = (
            existing_binding["api_key"]
            if existing_binding and existing_binding.get("api_key")
            else api_key_service.generate_api_key()
        )
        binding_repo.upsert_binding(
            store_id=store["id"],
            zendesk_subdomain=config.zendesk_subdomain,
            api_key=api_key,
        )

        logger.info("Tenant config saved: %s → %s", config.zendesk_subdomain, config.shopline_domain)
        return {
            "success": True,
            "data": {
                "message": "Configuration saved successfully",
                "handle": config.shopline_domain,
                "zendesk_subdomain": config.zendesk_subdomain,
            },
        }

    except httpx.TimeoutException:
        return {"success": False, "error": "Validation timeout - please check the domain"}
    except Exception as e:
        logger.error("Error setting up tenant config: %s", e)
        return {"success": False, "error": str(e)}
