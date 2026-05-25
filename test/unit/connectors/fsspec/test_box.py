"""Unit tests for the Box source connector's auth model."""

from __future__ import annotations

import pytest

from unstructured_ingest.processes.connectors.fsspec.box import (
    BoxAccessConfig,
    BoxConnectionConfig,
)


class TestBoxAccessConfigValidation:
    def test_jwt_only_is_valid(self):
        ac = BoxAccessConfig(box_app_config={"boxAppSettings": {"clientID": "x"}})
        assert ac.access_token is None
        assert ac.refresh_token is None

    def test_access_token_only_is_valid(self):
        ac = BoxAccessConfig(access_token="ya29.access")
        assert ac.box_app_config is None
        assert ac.refresh_token is None

    def test_access_token_and_refresh_token_is_valid(self):
        ac = BoxAccessConfig(access_token="ya29.access", refresh_token="long-lived-rt")
        assert ac.box_app_config is None
        assert ac.access_token == "ya29.access"
        assert ac.refresh_token == "long-lived-rt"

    def test_no_auth_method_rejected(self):
        with pytest.raises(ValueError, match="requires either"):
            BoxAccessConfig()

    def test_refresh_token_without_access_token_rejected(self):
        with pytest.raises(ValueError, match="requires either"):
            BoxAccessConfig(refresh_token="long-lived-rt")

    def test_box_app_config_with_access_token_rejected(self):
        with pytest.raises(ValueError, match="exactly one auth method"):
            BoxAccessConfig(
                box_app_config={"boxAppSettings": {}},
                access_token="ya29.access",
            )

    def test_box_app_config_with_refresh_token_rejected(self):
        with pytest.raises(ValueError, match="exactly one auth method"):
            BoxAccessConfig(
                box_app_config={"boxAppSettings": {}},
                refresh_token="long-lived-rt",
            )


class TestBoxConnectionConfigGetAccessConfig:
    def test_access_token_branch_returns_oauth_kwarg(self):
        cc = BoxConnectionConfig(
            access_config=BoxAccessConfig(access_token="live-token"),
        )
        kwargs = cc.get_access_config()
        assert "oauth" in kwargs
        assert kwargs["oauth"].access_token == "live-token"
        # secret fields stripped from the kwargs dict
        for k in ("access_token", "refresh_token", "box_app_config"):
            assert k not in kwargs

    def test_access_token_with_refresh_token_still_uses_access_token(self):
        cc = BoxConnectionConfig(
            access_config=BoxAccessConfig(
                access_token="live-token",
                refresh_token="long-lived-rt",
            ),
        )
        kwargs = cc.get_access_config()
        assert kwargs["oauth"].access_token == "live-token"

    def test_jwt_branch_unchanged(self, monkeypatch):
        from boxsdk import JWTAuth

        class _FakeJWT:
            def authenticate_instance(self):
                pass

        monkeypatch.setattr(
            JWTAuth,
            "from_settings_dictionary",
            staticmethod(lambda _settings: _FakeJWT()),
        )

        cc = BoxConnectionConfig(
            access_config=BoxAccessConfig(
                box_app_config={"boxAppSettings": {"clientID": "x"}},
            ),
        )
        kwargs = cc.get_access_config()
        assert "oauth" in kwargs
        assert "box_app_config" not in kwargs
