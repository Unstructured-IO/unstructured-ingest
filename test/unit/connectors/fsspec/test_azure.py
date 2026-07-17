import pytest
from pydantic import Secret

from unstructured_ingest.processes.connectors.fsspec.azure import (
    AzureAccessConfig,
    AzureConnectionConfig,
)


class TestAzureConnectionConfig:
    def test_get_access_config_defaults_to_anonymous_without_credentials(self):
        connection_config = AzureConnectionConfig(
            access_config=Secret(AzureAccessConfig(account_name="azureunstructured1"))
        )

        assert connection_config.get_access_config() == {
            "account_name": "azureunstructured1",
            "anon": True,
        }

    def test_get_access_config_uses_non_anonymous_mode_with_account_key(self):
        connection_config = AzureConnectionConfig(
            access_config=Secret(
                AzureAccessConfig(account_name="azureunstructured1", account_key="secret-key")
            )
        )

        assert connection_config.get_access_config() == {
            "account_name": "azureunstructured1",
            "account_key": "secret-key",
            "anon": False,
        }

    def test_get_access_config_uses_non_anonymous_mode_with_sas_token(self):
        connection_config = AzureConnectionConfig(
            access_config=Secret(
                AzureAccessConfig(account_name="azureunstructured1", sas_token="secret-sas")
            )
        )

        assert connection_config.get_access_config() == {
            "account_name": "azureunstructured1",
            "sas_token": "secret-sas",
            "anon": False,
        }

    def test_get_access_config_uses_non_anonymous_mode_with_connection_string(self):
        connection_config = AzureConnectionConfig(
            access_config=Secret(
                AzureAccessConfig(connection_string="UseDevelopmentStorage=true")
            )
        )

        assert connection_config.get_access_config() == {
            "connection_string": "UseDevelopmentStorage=true",
            "anon": False,
        }


_SECRET = "leaked-azure-reason-abc123XYZ"


class TestAzureWrapErrorRedaction:
    def _connection_config(self) -> AzureConnectionConfig:
        return AzureConnectionConfig(
            access_config=Secret(AzureAccessConfig(account_name="azureunstructured1"))
        )

    def test_client_auth_error_redacts_reason(self):
        pytest.importorskip("azure.core")
        from azure.core.exceptions import ClientAuthenticationError

        from unstructured_ingest.error import UserAuthError

        error = ClientAuthenticationError()
        error.reason = f"auth failed {_SECRET}"

        wrapped = self._connection_config().wrap_error(error)

        assert isinstance(wrapped, UserAuthError)
        assert _SECRET not in str(wrapped)

    def test_client_error_redacts_reason(self):
        pytest.importorskip("azure.core")
        from azure.core.exceptions import HttpResponseError

        from unstructured_ingest.error import UserError

        error = HttpResponseError()
        error.status_code = 403
        error.reason = f"forbidden {_SECRET}"

        wrapped = self._connection_config().wrap_error(error)

        assert isinstance(wrapped, UserError)
        assert _SECRET not in str(wrapped)

    def test_server_error_redacts_reason(self):
        pytest.importorskip("azure.core")
        from azure.core.exceptions import HttpResponseError

        from unstructured_ingest.error import ProviderError

        error = HttpResponseError()
        error.status_code = 500
        error.reason = f"server error {_SECRET}"

        wrapped = self._connection_config().wrap_error(error)

        assert isinstance(wrapped, ProviderError)
        assert _SECRET not in str(wrapped)
