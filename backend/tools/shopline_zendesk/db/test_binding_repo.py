"""Unit tests for binding_repo multi-store helpers."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from backend.tools.shopline_zendesk.db import binding_repo


_NOW = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
_ROW_ID = uuid.uuid4()
_STORE_ID = uuid.uuid4()

_ROW = (
    _ROW_ID,
    _STORE_ID,
    "acme",
    "api_key_123",
    "admin@example.com",
    "token_123",
    None,
    None,
    None,
    _NOW,
    _NOW,
    "store-a",
)


def _mock_connection(fetchone_return=None, fetchall_return=None, rowcount=0):
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


class TestListBindingsBySubdomain:
    @patch("backend.tools.shopline_zendesk.db.binding_repo.get_connection")
    def test_returns_all_bindings(self, mock_get_conn):
        ctx, _ = _mock_connection(fetchall_return=[_ROW, _ROW[:-1] + ("store-b",)])
        mock_get_conn.return_value = ctx

        result = binding_repo.list_bindings_by_subdomain("acme")

        assert len(result) == 2
        assert result[0]["handle"] == "store-a"
        assert result[1]["handle"] == "store-b"

    @patch("backend.tools.shopline_zendesk.db.binding_repo.get_connection")
    def test_sql_orders_by_updated_at(self, mock_get_conn):
        ctx, cur = _mock_connection(fetchall_return=[])
        mock_get_conn.return_value = ctx

        binding_repo.list_bindings_by_subdomain("acme")

        sql = cur.execute.call_args[0][0]
        assert "ORDER BY b.updated_at DESC, s.handle ASC" in sql


class TestGetBindingBySubdomainAndHandle:
    @patch("backend.tools.shopline_zendesk.db.binding_repo.get_connection")
    def test_returns_matching_binding(self, mock_get_conn):
        ctx, _ = _mock_connection(fetchone_return=_ROW)
        mock_get_conn.return_value = ctx

        result = binding_repo.get_binding_by_subdomain_and_handle("acme", "store-a")

        assert result is not None
        assert result["zendesk_subdomain"] == "acme"
        assert result["handle"] == "store-a"


class TestDeleteBindingBySubdomainAndHandle:
    @patch("backend.tools.shopline_zendesk.db.binding_repo.get_connection")
    def test_returns_true_when_deleted(self, mock_get_conn):
        ctx, _ = _mock_connection(rowcount=1)
        mock_get_conn.return_value = ctx

        assert binding_repo.delete_binding_by_subdomain_and_handle("acme", "store-a") is True

    @patch("backend.tools.shopline_zendesk.db.binding_repo.get_connection")
    def test_returns_false_when_missing(self, mock_get_conn):
        ctx, _ = _mock_connection(rowcount=0)
        mock_get_conn.return_value = ctx

        assert binding_repo.delete_binding_by_subdomain_and_handle("acme", "missing") is False
