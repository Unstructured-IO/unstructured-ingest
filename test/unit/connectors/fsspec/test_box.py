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


_SECRET = "leaked-box-message-abc123XYZ"


def _box_connection_config():
    from pydantic import Secret

    from unstructured_ingest.processes.connectors.fsspec.box import BoxConnectionConfig

    return BoxConnectionConfig(
        access_config=Secret(BoxAccessConfig(access_token="ya29.access")),
        remote_url="box://test",
    )


class TestBoxWrapErrorRedaction:
    def test_client_error_redacts_message(self):
        pytest.importorskip("boxsdk")
        from boxsdk.exception import BoxAPIException

        from unstructured_ingest.error import UserError

        error = BoxAPIException(401, message=f"unauthorized {_SECRET}")

        wrapped = _box_connection_config().wrap_error(error)

        assert isinstance(wrapped, UserError)
        assert _SECRET not in str(wrapped)

    def test_server_error_redacts_message(self):
        pytest.importorskip("boxsdk")
        from boxsdk.exception import BoxAPIException

        from unstructured_ingest.error import ProviderError

        error = BoxAPIException(500, message=f"server error {_SECRET}")

        wrapped = _box_connection_config().wrap_error(error)

        assert isinstance(wrapped, ProviderError)
        assert _SECRET not in str(wrapped)
