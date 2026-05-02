"""Unit tests for subscription tenant resolution."""

from __future__ import annotations

import importlib.util
import os
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import HTTPException

_REPO_ROOT = Path(__file__).resolve().parents[4]
_MODULE_PATH = _REPO_ROOT / "backend/tools/shopline_zendesk/routes/zendesk/app/routers/subscriptions.py"

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")


def _install_package_stub(name: str, path: Path) -> None:
    if name in sys.modules:
        return

    module = types.ModuleType(name)
    module.__path__ = [str(path)]
    sys.modules[name] = module


_install_package_stub(
    "backend.tools.shopline_zendesk.routes",
    _MODULE_PATH.parents[3],
)
_install_package_stub(
    "backend.tools.shopline_zendesk.routes.zendesk.app.routers",
    _MODULE_PATH.parent,
)

if "backend.tools.shopline_zendesk.routes.zendesk.app.database" not in sys.modules:
    database_stub = types.ModuleType(
        "backend.tools.shopline_zendesk.routes.zendesk.app.database"
    )

    def _unused_get_db():
        raise RuntimeError("database stub should not be called in this test")

    database_stub.get_db = _unused_get_db
    sys.modules["backend.tools.shopline_zendesk.routes.zendesk.app.database"] = (
        database_stub
    )

_SPEC = importlib.util.spec_from_file_location(
    "backend.tools.shopline_zendesk.routes.zendesk.app.routers.subscriptions",
    _MODULE_PATH,
)
assert _SPEC and _SPEC.loader
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
_resolve_tenant_id = _MODULE._resolve_tenant_id


def _make_request(
    *,
    tenant_store_id: str | None = None,
    zendesk_subdomain: str | None = "acme",
    shopline_handle: str | None = None,
):
    return SimpleNamespace(
        state=SimpleNamespace(tenant_store_id=tenant_store_id),
        headers={
            **({"X-Zendesk-Subdomain": zendesk_subdomain} if zendesk_subdomain else {}),
            **({"X-Shopline-Handle": shopline_handle} if shopline_handle else {}),
        },
        query_params={},
    )


class TestResolveTenantId:
    @patch.object(_MODULE.binding_repo, "list_bindings_by_subdomain")
    def test_uses_request_state_first(self, mock_list_bindings):
        request = _make_request(tenant_store_id="store-1")

        assert _resolve_tenant_id(request) == "store-1"
        mock_list_bindings.assert_not_called()

    @patch.object(_MODULE.binding_repo, "get_binding_by_subdomain_and_handle")
    def test_uses_explicit_handle(self, mock_get_binding):
        mock_get_binding.return_value = {"store_id": "store-2"}
        request = _make_request(shopline_handle="beta")

        assert _resolve_tenant_id(request) == "store-2"
        mock_get_binding.assert_called_once_with("acme", "beta")

    @patch.object(_MODULE.binding_repo, "list_bindings_by_subdomain")
    def test_requires_selection_when_multiple_bindings_exist(self, mock_list_bindings):
        mock_list_bindings.return_value = [
            {"store_id": "store-1", "handle": "alpha"},
            {"store_id": "store-2", "handle": "beta"},
        ]
        request = _make_request()

        with pytest.raises(HTTPException) as exc_info:
            _resolve_tenant_id(request)

        assert exc_info.value.status_code == 409
        assert exc_info.value.detail["code"] == "STORE_SELECTION_REQUIRED"

    @patch.object(_MODULE.binding_repo, "list_bindings_by_subdomain")
    def test_returns_single_binding_without_handle(self, mock_list_bindings):
        mock_list_bindings.return_value = [
            {"store_id": "store-9", "handle": "gamma"},
        ]
        request = _make_request()

        assert _resolve_tenant_id(request) == "store-9"
