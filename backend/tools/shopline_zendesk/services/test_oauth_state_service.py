"""Unit tests for oauth_state_service module."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from backend.tools.shopline_zendesk.services import oauth_state_service


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_NOW = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
_SAMPLE_ID = uuid.uuid4()


def _make_state_record(
    state: str = "abc123",
    handle: str = "mystore",
    subdomain: str = "acme",
    created_at: datetime | None = None,
    expires_at: datetime | None = None,
) -> dict:
    """Build a fake oauth_states record dict."""
    return {
        "id": _SAMPLE_ID,
        "state": state,
        "zendesk_subdomain": subdomain,
        "handle": handle,
        "created_at": created_at or _NOW,
        "expires_at": expires_at or (_NOW + timedelta(minutes=10)),
    }


# ---------------------------------------------------------------------------
# generate_state
# ---------------------------------------------------------------------------


class TestGenerateState:
    """Tests for generate_state()."""

    @patch("backend.tools.shopline_zendesk.services.oauth_state_service.oauth_state_repo")
    def test_returns_hex_string_at_least_32_chars(self, mock_repo):
        mock_repo.create_state.return_value = _make_state_record()

        state = oauth_state_service.generate_state("acme", "mystore")

        assert isinstance(state, str)
        assert len(state) >= 32

    @patch("backend.tools.shopline_zendesk.services.oauth_state_service.oauth_state_repo")
    def test_calls_create_state_with_correct_args(self, mock_repo):
        mock_repo.create_state.return_value = _make_state_record()

        state = oauth_state_service.generate_state("acme", "mystore")

        mock_repo.create_state.assert_called_once()
        call_kwargs = mock_repo.create_state.call_args[1]
        assert call_kwargs["state"] == state
        assert call_kwargs["zendesk_subdomain"] == "acme"
        assert call_kwargs["handle"] == "mystore"
        # expires_at should be ~10 minutes in the future
        assert call_kwargs["expires_at"].tzinfo is not None

    @patch("backend.tools.shopline_zendesk.services.oauth_state_service.oauth_state_repo")
    def test_generates_unique_states(self, mock_repo):
        mock_repo.create_state.return_value = _make_state_record()

        states = {oauth_state_service.generate_state("acme", "mystore") for _ in range(20)}
        assert len(states) == 20


# ---------------------------------------------------------------------------
# verify_state
# ---------------------------------------------------------------------------


class TestVerifyState:
    """Tests for verify_state()."""

    @patch("backend.tools.shopline_zendesk.services.oauth_state_service.datetime")
    @patch("backend.tools.shopline_zendesk.services.oauth_state_service.oauth_state_repo")
    def test_returns_true_for_valid_state(self, mock_repo, mock_dt):
        mock_dt.now.return_value = _NOW
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        record = _make_state_record(
            state="valid_state",
            handle="mystore",
            expires_at=_NOW + timedelta(minutes=5),
        )
        mock_repo.get_state.return_value = record

        result = oauth_state_service.verify_state("valid_state", "mystore")
        assert result is True
        mock_repo.delete_state.assert_called_once_with("valid_state")

    @patch("backend.tools.shopline_zendesk.services.oauth_state_service.oauth_state_repo")
    def test_returns_false_when_state_not_found(self, mock_repo):
        mock_repo.get_state.return_value = None

        result = oauth_state_service.verify_state("nonexistent", "mystore")
        assert result is False

    @patch("backend.tools.shopline_zendesk.services.oauth_state_service.datetime")
    @patch("backend.tools.shopline_zendesk.services.oauth_state_service.oauth_state_repo")
    def test_returns_false_when_handle_mismatch(self, mock_repo, mock_dt):
        mock_dt.now.return_value = _NOW
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        record = _make_state_record(handle="store-a")
        mock_repo.get_state.return_value = record

        result = oauth_state_service.verify_state("abc123", "store-b")
        assert result is False

    @patch("backend.tools.shopline_zendesk.services.oauth_state_service.datetime")
    @patch("backend.tools.shopline_zendesk.services.oauth_state_service.oauth_state_repo")
    def test_returns_false_when_expired(self, mock_repo, mock_dt):
        # Set "now" to after the expiry
        mock_dt.now.return_value = _NOW + timedelta(minutes=15)
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        record = _make_state_record(
            handle="mystore",
            expires_at=_NOW + timedelta(minutes=10),
        )
        mock_repo.get_state.return_value = record

        result = oauth_state_service.verify_state("abc123", "mystore")
        assert result is False
        # Expired state should be cleaned up
        mock_repo.delete_state.assert_called_once()


# ---------------------------------------------------------------------------
# cleanup_expired_states
# ---------------------------------------------------------------------------


class TestCleanupExpiredStates:
    """Tests for cleanup_expired_states()."""

    @patch("backend.tools.shopline_zendesk.services.oauth_state_service.oauth_state_repo")
    def test_delegates_to_repo(self, mock_repo):
        mock_repo.cleanup_expired_states.return_value = 3

        result = oauth_state_service.cleanup_expired_states()
        assert result == 3
        mock_repo.cleanup_expired_states.assert_called_once()

    @patch("backend.tools.shopline_zendesk.services.oauth_state_service.oauth_state_repo")
    def test_returns_zero_when_none_expired(self, mock_repo):
        mock_repo.cleanup_expired_states.return_value = 0

        result = oauth_state_service.cleanup_expired_states()
        assert result == 0
