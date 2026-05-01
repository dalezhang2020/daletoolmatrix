"""Unit tests for oauth_state_repo module.

These tests mock the database connection to verify SQL logic and
dict conversion without requiring a live Neon PostgreSQL instance.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from backend.tools.shopline_zendesk.db import oauth_state_repo


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_NOW = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
_EXPIRES = datetime(2025, 1, 15, 12, 10, 0, tzinfo=timezone.utc)
_SAMPLE_ID = uuid.uuid4()

_SAMPLE_ROW = (
    _SAMPLE_ID,
    "abc123state",
    "acme",
    "mystore",
    _NOW,
    _EXPIRES,
)


def _mock_connection(fetchone_return=None, fetchall_return=None, rowcount=0):
    """Build a mock get_connection context manager."""
    mock_cur = MagicMock()
    mock_cur.fetchone.return_value = fetchone_return
    mock_cur.fetchall.return_value = fetchall_return or []
    mock_cur.rowcount = rowcount

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=mock_conn)
    mock_ctx.__exit__ = MagicMock(return_value=False)
    return mock_ctx, mock_cur


# ---------------------------------------------------------------------------
# create_state
# ---------------------------------------------------------------------------


class TestCreateState:
    """Tests for create_state()."""

    @patch("backend.tools.shopline_zendesk.db.oauth_state_repo.get_connection")
    def test_returns_dict_with_all_fields(self, mock_get_conn):
        ctx, cur = _mock_connection(fetchone_return=_SAMPLE_ROW)
        mock_get_conn.return_value = ctx

        result = oauth_state_repo.create_state(
            state="abc123state",
            zendesk_subdomain="acme",
            handle="mystore",
            expires_at=_EXPIRES,
        )

        assert result["state"] == "abc123state"
        assert result["zendesk_subdomain"] == "acme"
        assert result["handle"] == "mystore"
        assert result["expires_at"] == _EXPIRES
        assert result["id"] == _SAMPLE_ID

    @patch("backend.tools.shopline_zendesk.db.oauth_state_repo.get_connection")
    def test_executes_insert_sql(self, mock_get_conn):
        ctx, cur = _mock_connection(fetchone_return=_SAMPLE_ROW)
        mock_get_conn.return_value = ctx

        oauth_state_repo.create_state("s", "sub", "h", _EXPIRES)

        cur.execute.assert_called_once()
        sql = cur.execute.call_args[0][0]
        assert "INSERT INTO shopline_zendesk.oauth_states" in sql


# ---------------------------------------------------------------------------
# get_state
# ---------------------------------------------------------------------------


class TestGetState:
    """Tests for get_state()."""

    @patch("backend.tools.shopline_zendesk.db.oauth_state_repo.get_connection")
    def test_returns_dict_when_found(self, mock_get_conn):
        ctx, cur = _mock_connection(fetchone_return=_SAMPLE_ROW)
        mock_get_conn.return_value = ctx

        result = oauth_state_repo.get_state("abc123state")
        assert result is not None
        assert result["state"] == "abc123state"
        assert result["handle"] == "mystore"

    @patch("backend.tools.shopline_zendesk.db.oauth_state_repo.get_connection")
    def test_returns_none_when_not_found(self, mock_get_conn):
        ctx, cur = _mock_connection(fetchone_return=None)
        mock_get_conn.return_value = ctx

        result = oauth_state_repo.get_state("nonexistent")
        assert result is None


# ---------------------------------------------------------------------------
# delete_state
# ---------------------------------------------------------------------------


class TestDeleteState:
    """Tests for delete_state()."""

    @patch("backend.tools.shopline_zendesk.db.oauth_state_repo.get_connection")
    def test_returns_true_when_deleted(self, mock_get_conn):
        ctx, cur = _mock_connection(rowcount=1)
        mock_get_conn.return_value = ctx

        assert oauth_state_repo.delete_state("abc123state") is True

    @patch("backend.tools.shopline_zendesk.db.oauth_state_repo.get_connection")
    def test_returns_false_when_not_found(self, mock_get_conn):
        ctx, cur = _mock_connection(rowcount=0)
        mock_get_conn.return_value = ctx

        assert oauth_state_repo.delete_state("nonexistent") is False


# ---------------------------------------------------------------------------
# cleanup_expired_states
# ---------------------------------------------------------------------------


class TestCleanupExpiredStates:
    """Tests for cleanup_expired_states()."""

    @patch("backend.tools.shopline_zendesk.db.oauth_state_repo.get_connection")
    def test_returns_count_of_deleted_rows(self, mock_get_conn):
        ctx, cur = _mock_connection(rowcount=5)
        mock_get_conn.return_value = ctx

        assert oauth_state_repo.cleanup_expired_states() == 5

    @patch("backend.tools.shopline_zendesk.db.oauth_state_repo.get_connection")
    def test_returns_zero_when_none_expired(self, mock_get_conn):
        ctx, cur = _mock_connection(rowcount=0)
        mock_get_conn.return_value = ctx

        assert oauth_state_repo.cleanup_expired_states() == 0
