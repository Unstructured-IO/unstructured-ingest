import pytest

from unstructured_ingest.error import ValueError
from unstructured_ingest.processes.connectors.outlook import (
    OutlookAccessConfig,
)


class TestOutlookAccessConfig:
    """Tests for OutlookAccessConfig authentication validation."""

    def test_client_cred_only(self):
        """Client credential alone should be valid (app-only authentication)."""
        config = OutlookAccessConfig(client_cred="secret-value")
        # `client_credential` is the field name; `client_cred` is the alias.
        assert config.client_credential == "secret-value"
        assert config.oauth_token is None

    def test_oauth_token_only(self):
        """OAuth token alone should be valid (delegated authentication)."""
        config = OutlookAccessConfig(oauth_token="ey.access.token")
        assert config.oauth_token == "ey.access.token"
        assert config.client_credential is None

    def test_no_auth_raises_error(self):
        """No authentication provided should raise ValueError."""
        with pytest.raises(ValueError, match="must be set"):
            OutlookAccessConfig()

    def test_oauth_and_client_cred_raises_error(self):
        """Both oauth_token and client_cred provided should raise ValueError."""
        with pytest.raises(ValueError, match="cannot use both"):
            OutlookAccessConfig(
                client_cred="secret-value",
                oauth_token="ey.access.token",
            )
