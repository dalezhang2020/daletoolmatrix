"""Unit tests for validators module."""

import pytest
from backend.tools.shopline_zendesk.services.validators import (
    build_oauth_popup_url,
    build_shopline_auth_url,
    validate_handle,
    validate_zendesk_subdomain,
)


# ---------------------------------------------------------------------------
# validate_handle
# ---------------------------------------------------------------------------


class TestValidateHandle:
    """Tests for validate_handle()."""

    def test_simple_alphanumeric(self):
        assert validate_handle("mystore") == "mystore"

    def test_with_hyphens_and_underscores(self):
        assert validate_handle("my-store_01") == "my-store_01"

    def test_single_char(self):
        assert validate_handle("a") == "a"

    def test_max_length_64(self):
        handle = "a" * 64
        assert validate_handle(handle) == handle

    def test_too_long_raises(self):
        with pytest.raises(ValueError, match="Invalid store handle"):
            validate_handle("a" * 65)

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="Invalid store handle"):
            validate_handle("")

    def test_special_chars_raise(self):
        for bad in ["my store", "store@1", "store.com", "store/path", "store\n"]:
            with pytest.raises(ValueError):
                validate_handle(bad)

    def test_non_string_raises(self):
        with pytest.raises(ValueError):
            validate_handle(123)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# validate_zendesk_subdomain
# ---------------------------------------------------------------------------


class TestValidateZendeskSubdomain:
    """Tests for validate_zendesk_subdomain()."""

    def test_simple_alphanumeric(self):
        assert validate_zendesk_subdomain("acme") == "acme"

    def test_with_hyphens(self):
        assert validate_zendesk_subdomain("acme-corp") == "acme-corp"

    def test_single_char(self):
        assert validate_zendesk_subdomain("x") == "x"

    def test_max_length_64(self):
        sub = "a" * 64
        assert validate_zendesk_subdomain(sub) == sub

    def test_too_long_raises(self):
        with pytest.raises(ValueError, match="Invalid Zendesk subdomain"):
            validate_zendesk_subdomain("a" * 65)

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="Invalid Zendesk subdomain"):
            validate_zendesk_subdomain("")

    def test_underscores_rejected(self):
        """Zendesk subdomains don't allow underscores (unlike handles)."""
        with pytest.raises(ValueError):
            validate_zendesk_subdomain("acme_corp")

    def test_special_chars_raise(self):
        for bad in ["acme corp", "acme.com", "acme/path"]:
            with pytest.raises(ValueError):
                validate_zendesk_subdomain(bad)

    def test_non_string_raises(self):
        with pytest.raises(ValueError):
            validate_zendesk_subdomain(None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# build_oauth_popup_url
# ---------------------------------------------------------------------------


class TestBuildOAuthPopupUrl:
    """Tests for build_oauth_popup_url()."""

    def test_basic_url(self):
        url = build_oauth_popup_url("https://api.example.com", "acme")
        assert url == "https://api.example.com/oauth/shopline/start?zendesk_subdomain=acme"

    def test_trailing_slash_stripped(self):
        url = build_oauth_popup_url("https://api.example.com/", "acme")
        assert url == "https://api.example.com/oauth/shopline/start?zendesk_subdomain=acme"

    def test_invalid_subdomain_raises(self):
        with pytest.raises(ValueError):
            build_oauth_popup_url("https://api.example.com", "bad subdomain!")


# ---------------------------------------------------------------------------
# build_shopline_auth_url
# ---------------------------------------------------------------------------


class TestBuildShoplineAuthUrl:
    """Tests for build_shopline_auth_url()."""

    def test_contains_handle_domain(self):
        url = build_shopline_auth_url(
            handle="mystore",
            app_key="key123",
            redirect_uri="https://api.example.com/callback",
            scopes="read_product write_order",
            state="abc123def456",
        )
        assert "mystore.myshopline.com" in url

    def test_contains_required_params(self):
        url = build_shopline_auth_url(
            handle="mystore",
            app_key="key123",
            redirect_uri="https://api.example.com/callback",
            scopes="read_product",
            state="state123",
        )
        assert "appKey=key123" in url
        assert "responseType=code" in url
        assert "scope=read_product" in url

    def test_state_in_redirect_uri(self):
        url = build_shopline_auth_url(
            handle="mystore",
            app_key="key123",
            redirect_uri="https://api.example.com/callback",
            scopes="read_product",
            state="mystate",
        )
        # The state should appear inside the redirectUri parameter value
        assert "state%3Dmystate" in url or "state=mystate" in url

    def test_invalid_handle_raises(self):
        with pytest.raises(ValueError):
            build_shopline_auth_url(
                handle="bad handle!",
                app_key="key",
                redirect_uri="https://example.com/cb",
                scopes="read",
                state="s",
            )

    def test_base_url_format(self):
        url = build_shopline_auth_url(
            handle="testshop",
            app_key="k",
            redirect_uri="https://example.com/cb",
            scopes="s",
            state="st",
        )
        assert url.startswith("https://testshop.myshopline.com/admin/oauth-web/#/oauth/authorize?")
