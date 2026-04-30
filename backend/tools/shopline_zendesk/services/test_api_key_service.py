"""Unit tests for api_key_service module."""

import pytest
from backend.tools.shopline_zendesk.services.api_key_service import (
    generate_api_key,
    verify_api_key,
)


class TestGenerateApiKey:
    """Tests for generate_api_key()."""

    def test_returns_64_char_hex_string(self):
        """32 bytes = 64 hex characters."""
        key = generate_api_key()
        assert len(key) == 64

    def test_only_hex_characters(self):
        key = generate_api_key()
        assert all(c in "0123456789abcdef" for c in key)

    def test_generates_unique_keys(self):
        """Two calls should produce different keys."""
        key1 = generate_api_key()
        key2 = generate_api_key()
        assert key1 != key2


class TestVerifyApiKey:
    """Tests for verify_api_key()."""

    def test_matching_keys_return_true(self):
        key = generate_api_key()
        assert verify_api_key(key, key) is True

    def test_different_keys_return_false(self):
        key1 = generate_api_key()
        key2 = generate_api_key()
        assert verify_api_key(key1, key2) is False

    def test_empty_strings_match(self):
        assert verify_api_key("", "") is True

    def test_empty_vs_nonempty_returns_false(self):
        key = generate_api_key()
        assert verify_api_key("", key) is False
        assert verify_api_key(key, "") is False

    def test_prefix_does_not_match(self):
        key = generate_api_key()
        assert verify_api_key(key[:32], key) is False

    def test_suffix_does_not_match(self):
        key = generate_api_key()
        assert verify_api_key(key[32:], key) is False
