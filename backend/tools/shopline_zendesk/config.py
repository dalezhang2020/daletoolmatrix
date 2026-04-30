"""Environment config validation for Shopline-Zendesk tool."""
from __future__ import annotations

import os
from typing import Dict, List, Optional

_REQUIRED: Dict[str, str] = {
    "SHOPLINE_ZD_APP_KEY": "Shopline-Zendesk app key",
    "SHOPLINE_ZD_APP_SECRET": "Shopline-Zendesk app secret",
    "DATABASE_URL": "Neon/PostgreSQL connection string",
    "SHOPLINE_ZD_FRONTEND_URL": "Vercel frontend URL for post-OAuth redirect",
}

# Optional env vars with defaults
_OPTIONAL_DEFAULTS: Dict[str, str] = {
    "SHOPLINE_ZD_SKIP_HMAC": "0",  # Set to "1" in local dev to skip HMAC verification
}


def validate_env(keys: Optional[List[str]] = None) -> None:
    """Validate that required environment variables are set.

    Args:
        keys: Specific keys to check. If None, checks all required keys.

    Raises:
        RuntimeError: If any required env vars are missing.
    """
    check = keys or list(_REQUIRED.keys())
    missing = [k for k in check if not os.environ.get(k)]
    if missing:
        descriptions = [f"  {k} — {_REQUIRED.get(k, 'required')}" for k in missing]
        raise RuntimeError("Missing required environment variables:\n" + "\n".join(descriptions))


def validate_shopline_zd() -> None:
    """Validate Shopline-Zendesk specific env vars."""
    validate_env(["SHOPLINE_ZD_APP_KEY", "SHOPLINE_ZD_APP_SECRET", "SHOPLINE_ZD_FRONTEND_URL"])


def validate_database() -> None:
    """Validate database connection env var."""
    validate_env(["DATABASE_URL"])


def get_skip_hmac() -> bool:
    """Return whether HMAC verification should be skipped (local dev only)."""
    return os.environ.get("SHOPLINE_ZD_SKIP_HMAC", _OPTIONAL_DEFAULTS["SHOPLINE_ZD_SKIP_HMAC"]) == "1"
