"""Unit tests for the Box source connector's auth model."""

from __future__ import annotations

import pytest

from unstructured_ingest.processes.connectors.fsspec.box import (
    BoxAccessConfig,
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

    def test_model_validate_accepts_explicit_null_box_app_config(self):
        """The platform serializes optional fields as JSON null. The BeforeValidator
        on box_app_config must pass None through rather than raising."""
        ac = BoxAccessConfig.model_validate(
            {"box_app_config": None, "access_token": "ya29.access", "refresh_token": "rt"},
        )
        assert ac.box_app_config is None
        assert ac.access_token == "ya29.access"
