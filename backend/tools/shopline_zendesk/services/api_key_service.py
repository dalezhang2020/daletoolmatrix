"""API key generation and constant-time comparison."""

import hmac
import secrets


def generate_api_key() -> str:
    """Generate a cryptographically secure 32-byte random hex string (64 chars).

    Uses secrets.token_hex for secure random generation suitable for API keys.
    """
    return secrets.token_hex(32)


def verify_api_key(provided: str, stored: str) -> bool:
    """Constant-time comparison of two API key strings.

    Uses hmac.compare_digest to prevent timing attacks.
    """
    return hmac.compare_digest(provided, stored)
