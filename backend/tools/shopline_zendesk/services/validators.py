"""Input validation and URL construction for Shopline OAuth flows."""

from __future__ import annotations

import re
from urllib.parse import quote, urlencode

# Shopline store handle: alphanumeric, hyphens, underscores, 1-64 chars.
# Use \Z instead of $ to reject trailing newlines.
_HANDLE_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}\Z")

# Zendesk subdomain: alphanumeric and hyphens only, 1-64 chars.
# Use \Z instead of $ to reject trailing newlines.
_SUBDOMAIN_RE = re.compile(r"^[a-zA-Z0-9-]{1,64}\Z")


def validate_handle(handle: str) -> str:
    """Validate and return a Shopline store handle.

    Accepts strings matching ``^[a-zA-Z0-9_-]{1,64}$``.

    Returns:
        The validated handle (unchanged).

    Raises:
        ValueError: If *handle* is empty, too long, or contains
            characters outside the allowed set.
    """
    if not isinstance(handle, str) or not _HANDLE_RE.match(handle):
        raise ValueError(
            f"Invalid store handle: must match ^[a-zA-Z0-9_-]{{1,64}}$, got {handle!r}"
        )
    return handle


def validate_zendesk_subdomain(subdomain: str) -> str:
    """Validate and return a Zendesk subdomain.

    Accepts strings matching ``^[a-zA-Z0-9-]{1,64}$``.

    Returns:
        The validated subdomain (unchanged).

    Raises:
        ValueError: If *subdomain* is empty, too long, or contains
            characters outside the allowed set.
    """
    if not isinstance(subdomain, str) or not _SUBDOMAIN_RE.match(subdomain):
        raise ValueError(
            f"Invalid Zendesk subdomain: must match ^[a-zA-Z0-9-]{{1,64}}$, got {subdomain!r}"
        )
    return subdomain


def build_oauth_popup_url(base_url: str, zendesk_subdomain: str) -> str:
    """Build the OAuth popup URL for the start endpoint.

    The returned URL has the form:
        ``{base_url}/oauth/shopline/start?zendesk_subdomain={subdomain}``

    The subdomain is validated before inclusion.

    Args:
        base_url: Backend base URL (e.g. ``https://api.example.com``).
        zendesk_subdomain: The Zendesk instance subdomain.

    Returns:
        Fully-qualified popup URL.

    Raises:
        ValueError: If *zendesk_subdomain* is invalid.
    """
    validated = validate_zendesk_subdomain(zendesk_subdomain)
    # Strip trailing slash from base_url to avoid double slashes.
    base = base_url.rstrip("/")
    return f"{base}/oauth/shopline/start?zendesk_subdomain={quote(validated, safe='')}"


def build_shopline_auth_url(
    handle: str,
    app_key: str,
    redirect_uri: str,
    scopes: str,
    state: str,
) -> str:
    """Build the Shopline OAuth authorization URL.

    The returned URL has the form::

        https://{handle}.myshopline.com/admin/oauth-web/#/oauth/authorize
            ?appKey={app_key}
            &responseType=code
            &scope={scopes}
            &redirectUri={redirect_uri_with_state}

    The *state* parameter is appended to *redirect_uri* as a query param
    so it round-trips through the OAuth callback for CSRF verification.

    Args:
        handle: Validated Shopline store handle.
        app_key: Shopline app key.
        redirect_uri: OAuth callback URL (without state param).
        scopes: Space-separated OAuth scopes.
        state: CSRF state token.

    Returns:
        Fully-qualified Shopline authorization URL.

    Raises:
        ValueError: If *handle* is invalid.
    """
    validated_handle = validate_handle(handle)

    # Append state to redirect_uri as a query parameter.
    separator = "&" if "?" in redirect_uri else "?"
    redirect_with_state = f"{redirect_uri}{separator}state={quote(state, safe='')}"

    params = urlencode(
        {
            "appKey": app_key,
            "responseType": "code",
            "scope": scopes,
            "redirectUri": redirect_with_state,
        },
        safe="",  # encode everything
    )

    base = f"https://{validated_handle}.myshopline.com/admin/oauth-web/#/oauth/authorize"
    return f"{base}?{params}"
