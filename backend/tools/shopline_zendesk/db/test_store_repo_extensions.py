"""Unit tests for store_repo extension functions (token refresh support).

Tests the four new functions added for the OAuth integration:
- get_expiring_stores
- increment_refresh_fail_count
- mark_token_invalid
- reset_refresh_fail_count
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from backend.tools.shopline_zendesk.db import store_repo


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_NOW = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
_SAMPLE_ID = uuid.uuid4()

# Extended row: id, handle, access_token, expires_at, scopes, installed_at, updated_at,
#               refresh_fail_count, token_invalid
_EXTENDED_ROW = (
    _SAMPLE_ID,
    "mystore",
    "tok_abc123",
    _NOW,
    "read_product",
    _NOW,
    _NOW,
    0,
    False,
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
# get_expiring_stores
# ---------------------------------------------------------------------------


class TestGetExpiringStores:
    """Tests for get_expiring_stores()."""

    @patch("backend.tools.shopline_zendesk.db.store_repo.get_connection")
    def test_returns_list_of_dicts(self, mock_get_conn):
        ctx, cur = _mock_connection(fetchall_return=[_EXTENDED_ROW])
        mock_get_conn.return_value = ctx

        result = store_repo.get_expiring_stores(hours=2)
        assert len(result) == 1
        assert result[0]["handle"] == "mystore"
        assert result[0]["refresh_fail_count"] == 0
        assert result[0]["token_invalid"] is False

    @patch("backend.tools.shopline_zendesk.db.store_repo.get_connection")
    def test_returns_empty_list_when_none(self, mock_get_conn):
        ctx, cur = _mock_connection(fetchall_return=[])
        mock_get_conn.return_value = ctx

        result = store_repo.get_expiring_stores()
        assert result == []

    @patch("backend.tools.shopline_zendesk.db.store_repo.get_connection")
    def test_passes_hours_param_to_sql(self, mock_get_conn):
        ctx, cur = _mock_connection(fetchall_return=[])
        mock_get_conn.return_value = ctx

        store_repo.get_expiring_stores(hours=4)
        sql_args = cur.execute.call_args[0][1]
        assert sql_args == (4,)

    @patch("backend.tools.shopline_zendesk.db.store_repo.get_connection")
    def test_sql_filters_token_invalid(self, mock_get_conn):
        ctx, cur = _mock_connection(fetchall_return=[])
        mock_get_conn.return_value = ctx

        store_repo.get_expiring_stores()
        sql = cur.execute.call_args[0][0]
        assert "token_invalid = FALSE" in sql


# ---------------------------------------------------------------------------
# increment_refresh_fail_count
# ---------------------------------------------------------------------------


class TestIncrementRefreshFailCount:
    """Tests for increment_refresh_fail_count()."""

    @patch("backend.tools.shopline_zendesk.db.store_repo.get_connection")
    def test_returns_updated_dict(self, mock_get_conn):
        row = (*_EXTENDED_ROW[:7], 1, False)  # fail_count = 1
        ctx, cur = _mock_connection(fetchone_return=row)
        mock_get_conn.return_value = ctx

        result = store_repo.increment_refresh_fail_count("mystore")
        assert result is not None
        assert result["refresh_fail_count"] == 1

    @patch("backend.tools.shopline_zendesk.db.store_repo.get_connection")
    def test_returns_none_when_not_found(self, mock_get_conn):
        ctx, cur = _mock_connection(fetchone_return=None)
        mock_get_conn.return_value = ctx

        result = store_repo.increment_refresh_fail_count("nonexistent")
        assert result is None

    @patch("backend.tools.shopline_zendesk.db.store_repo.get_connection")
    def test_sql_increments_by_one(self, mock_get_conn):
        ctx, cur = _mock_connection(fetchone_return=_EXTENDED_ROW)
        mock_get_conn.return_value = ctx

        store_repo.increment_refresh_fail_count("mystore")
        sql = cur.execute.call_args[0][0]
        assert "refresh_fail_count + 1" in sql


# ---------------------------------------------------------------------------
# mark_token_invalid
# ---------------------------------------------------------------------------


class TestMarkTokenInvalid:
    """Tests for mark_token_invalid()."""

    @patch("backend.tools.shopline_zendesk.db.store_repo.get_connection")
    def test_returns_updated_dict(self, mock_get_conn):
        row = (*_EXTENDED_ROW[:8], True)  # token_invalid = True
        ctx, cur = _mock_connection(fetchone_return=row)
        mock_get_conn.return_value = ctx

        result = store_repo.mark_token_invalid("mystore")
        assert result is not None
        assert result["token_invalid"] is True

    @patch("backend.tools.shopline_zendesk.db.store_repo.get_connection")
    def test_returns_none_when_not_found(self, mock_get_conn):
        ctx, cur = _mock_connection(fetchone_return=None)
        mock_get_conn.return_value = ctx

        result = store_repo.mark_token_invalid("nonexistent")
        assert result is None


# ---------------------------------------------------------------------------
# reset_refresh_fail_count
# ---------------------------------------------------------------------------


class TestResetRefreshFailCount:
    """Tests for reset_refresh_fail_count()."""

    @patch("backend.tools.shopline_zendesk.db.store_repo.get_connection")
    def test_returns_updated_dict_with_zero_count(self, mock_get_conn):
        row = _EXTENDED_ROW  # fail_count = 0
        ctx, cur = _mock_connection(fetchone_return=row)
        mock_get_conn.return_value = ctx

        result = store_repo.reset_refresh_fail_count("mystore")
        assert result is not None
        assert result["refresh_fail_count"] == 0

    @patch("backend.tools.shopline_zendesk.db.store_repo.get_connection")
    def test_returns_none_when_not_found(self, mock_get_conn):
        ctx, cur = _mock_connection(fetchone_return=None)
        mock_get_conn.return_value = ctx

        result = store_repo.reset_refresh_fail_count("nonexistent")
        assert result is None

    @patch("backend.tools.shopline_zendesk.db.store_repo.get_connection")
    def test_sql_sets_count_to_zero(self, mock_get_conn):
        ctx, cur = _mock_connection(fetchone_return=_EXTENDED_ROW)
        mock_get_conn.return_value = ctx

        store_repo.reset_refresh_fail_count("mystore")
        sql = cur.execute.call_args[0][0]
        assert "refresh_fail_count = 0" in sql
