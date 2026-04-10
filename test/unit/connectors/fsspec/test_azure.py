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
