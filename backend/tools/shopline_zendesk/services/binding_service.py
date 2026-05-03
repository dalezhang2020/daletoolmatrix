"""Store-Zendesk binding CRUD logic.

Orchestrates binding_repo, store_repo, and api_key_service to manage
Shopline store bindings under a Zendesk subdomain.
"""

from __future__ import annotations

import logging

from backend.tools.shopline_zendesk.db import binding_repo, store_repo
from backend.tools.shopline_zendesk.services import api_key_service

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------


class StoreNotFoundError(Exception):
    """Raised when no store exists for the given handle."""


class BindingNotFoundError(Exception):
    """Raised when no binding exists for the given handle or subdomain."""


class InvalidApiKeyError(Exception):
    """Raised when the provided API key does not match the stored key."""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def create_or_update_binding(
    handle: str,
    zendesk_subdomain: str,
    zendesk_admin_email: str | None = None,
    zendesk_api_token: str | None = None,
) -> dict:
    """Create or update a store-Zendesk binding.

    Steps:
      1. Look up the store by handle to get its store_id.
      2. Generate a fresh API key.
      3. Upsert the binding row (insert or update on store_id conflict).
      4. Return the binding dict with the plaintext API key included.

    Raises:
        StoreNotFoundError: If no store exists for *handle*.
    """
    store = store_repo.get_store_by_handle(handle)
    if store is None:
        raise StoreNotFoundError(f"No store found for handle: {handle}")

    api_key = api_key_service.generate_api_key()
    binding = binding_repo.upsert_binding(
        store_id=store["id"],
        zendesk_subdomain=zendesk_subdomain,
        api_key=api_key,
        zendesk_admin_email=zendesk_admin_email,
        zendesk_api_token=zendesk_api_token,
    )

    # Attach the handle for convenience (the repo row doesn't include it).
    binding["handle"] = handle
    return binding


def update_zendesk_credentials(
    handle: str,
    zendesk_admin_email: str | None = None,
    zendesk_api_token: str | None = None,
) -> dict:
    """Update only the Zendesk admin credentials on an existing binding.

    This does NOT create a new binding or change the zendesk_subdomain.
    It only patches zendesk_admin_email and zendesk_api_token on the
    binding that was already created by the ZAF OAuth flow.

    Raises:
        StoreNotFoundError: If no store exists for *handle*.
        BindingNotFoundError: If no binding exists for *handle*.
    """
    store = store_repo.get_store_by_handle(handle)
    if store is None:
        raise StoreNotFoundError(f"No store found for handle: {handle}")

    binding = binding_repo.get_binding_by_handle(handle)
    if binding is None:
        raise BindingNotFoundError(f"No binding found for handle: {handle}")

    # Update only the credential fields, keep everything else intact
    updated = binding_repo.upsert_binding(
        store_id=str(store["id"]),
        zendesk_subdomain=binding["zendesk_subdomain"],
        api_key=binding["api_key"],
        zendesk_admin_email=zendesk_admin_email,
        zendesk_api_token=zendesk_api_token,
        zendesk_access_token=binding.get("zendesk_access_token"),
        zendesk_refresh_token=binding.get("zendesk_refresh_token"),
        zendesk_token_expires_at=binding.get("zendesk_token_expires_at"),
    )
    updated["handle"] = handle
    updated["managed_in_zaf"] = True
    updated["token_invalid"] = bool(store.get("token_invalid", False))
    # Strip sensitive fields
    updated["api_key"] = None
    return updated


def get_binding_status(handle: str) -> dict:
    """Return the current binding for a store, without exposing the API key.

    Returns a dict with ``api_key`` set to ``None`` so callers never
    accidentally leak the secret on read-only queries.

    Raises:
        BindingNotFoundError: If no binding exists for *handle*.
    """
    store = store_repo.get_store_by_handle(handle)
    token_invalid = bool(store.get("token_invalid", False)) if store else False

    binding = binding_repo.get_binding_by_handle(handle)
    if binding is None:
        # No binding yet is a normal state — return a "not configured" stub.
        return {
            "handle": handle,
            "zendesk_subdomain": None,
            "api_key": None,
            "has_zendesk_credentials": False,
            "managed_in_zaf": True,
            "token_invalid": token_invalid,
        }

    # Strip the API key from the response.
    binding["api_key"] = None
    binding["managed_in_zaf"] = True
    binding["token_invalid"] = token_invalid
    return binding


def resolve_store_from_subdomain(
    zendesk_subdomain: str,
    api_key: str,
    handle: str | None = None,
) -> dict:
    """Authenticate a Zendesk request and resolve the backing Shopline store.

    Steps:
      1. Look up the binding by Zendesk subdomain.
      2. Verify the provided API key against the stored key (constant-time).
      3. Fetch the full store record so the caller has access_token, etc.

    Returns:
        The store dict (id, handle, access_token, expires_at, …).

    Raises:
        BindingNotFoundError: If no binding exists for *zendesk_subdomain*.
        InvalidApiKeyError: If the API key does not match.
        StoreNotFoundError: If the store referenced by the binding no longer
            exists (should not happen under normal operation).
    """
    if handle:
        candidate_bindings = []
        scoped_binding = binding_repo.get_binding_by_subdomain_and_handle(
            zendesk_subdomain=zendesk_subdomain,
            handle=handle,
        )
        if scoped_binding is not None:
            candidate_bindings.append(scoped_binding)
    else:
        candidate_bindings = binding_repo.list_bindings_by_subdomain(zendesk_subdomain)

    if not candidate_bindings:
        raise BindingNotFoundError(
            f"No binding found for subdomain: {zendesk_subdomain}"
        )

    binding = next(
        (
            candidate
            for candidate in candidate_bindings
            if api_key_service.verify_api_key(api_key, candidate["api_key"])
        ),
        None,
    )
    if binding is None:
        raise InvalidApiKeyError("API key verification failed")

    store = store_repo.get_store_by_id(binding["store_id"])
    if store is None:
        raise StoreNotFoundError(
            f"Store {binding['store_id']} referenced by binding no longer exists"
        )

    return store
