"""Unit tests for binding_service multi-store resolution."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from backend.tools.shopline_zendesk.services import binding_service


def _binding(store_id: str, handle: str, api_key: str) -> dict:
    return {
        "store_id": store_id,
        "handle": handle,
        "api_key": api_key,
        "zendesk_subdomain": "acme",
    }


class TestResolveStoreFromSubdomain:
    @patch("backend.tools.shopline_zendesk.services.binding_service.store_repo.get_store_by_id")
    @patch("backend.tools.shopline_zendesk.services.binding_service.api_key_service.verify_api_key")
    @patch("backend.tools.shopline_zendesk.services.binding_service.binding_repo.list_bindings_by_subdomain")
    def test_resolves_matching_store_across_multiple_bindings(
        self,
        mock_list_bindings,
        mock_verify,
        mock_get_store,
    ):
        mock_list_bindings.return_value = [
            _binding("store-1", "alpha", "key-1"),
            _binding("store-2", "beta", "key-2"),
        ]
        mock_verify.side_effect = lambda provided, stored: provided == stored
        mock_get_store.return_value = {"id": "store-2", "handle": "beta", "access_token": "tok"}

        result = binding_service.resolve_store_from_subdomain("acme", "key-2")

        assert result["handle"] == "beta"
        mock_get_store.assert_called_once_with("store-2")

    @patch("backend.tools.shopline_zendesk.services.binding_service.store_repo.get_store_by_id")
    @patch("backend.tools.shopline_zendesk.services.binding_service.api_key_service.verify_api_key")
    @patch("backend.tools.shopline_zendesk.services.binding_service.binding_repo.get_binding_by_subdomain_and_handle")
    def test_uses_handle_scope_when_provided(
        self,
        mock_get_binding,
        mock_verify,
        mock_get_store,
    ):
        mock_get_binding.return_value = _binding("store-9", "gamma", "key-9")
        mock_verify.return_value = True
        mock_get_store.return_value = {"id": "store-9", "handle": "gamma", "access_token": "tok"}

        result = binding_service.resolve_store_from_subdomain("acme", "key-9", handle="gamma")

        assert result["id"] == "store-9"
        mock_get_binding.assert_called_once_with(
            zendesk_subdomain="acme",
            handle="gamma",
        )

    @patch("backend.tools.shopline_zendesk.services.binding_service.api_key_service.verify_api_key")
    @patch("backend.tools.shopline_zendesk.services.binding_service.binding_repo.list_bindings_by_subdomain")
    def test_raises_invalid_api_key_when_none_match(
        self,
        mock_list_bindings,
        mock_verify,
    ):
        mock_list_bindings.return_value = [_binding("store-1", "alpha", "key-1")]
        mock_verify.return_value = False

        with pytest.raises(binding_service.InvalidApiKeyError):
            binding_service.resolve_store_from_subdomain("acme", "wrong-key")
