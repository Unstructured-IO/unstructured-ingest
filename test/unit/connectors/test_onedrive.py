import pytest

from unstructured_ingest.error import ValueError
from unstructured_ingest.processes.connectors.onedrive import (
    OnedriveAccessConfig,
)


class TestOnedriveAccessConfig:
    """Tests for OnedriveAccessConfig authentication validation."""

    def test_client_cred_only(self):
        """Client credential alone should be valid (app-only authentication)."""
        config = OnedriveAccessConfig(client_cred="secret-value")
        assert config.client_cred == "secret-value"
        assert config.oauth_token is None

    def test_client_cred_and_password(self):
        """client_cred + password is the password-grant flow and should be valid."""
        config = OnedriveAccessConfig(client_cred="secret-value", password="user-password")
        assert config.client_cred == "secret-value"
        assert config.password == "user-password"
        assert config.oauth_token is None

    def test_oauth_token_only(self):
        """OAuth token alone should be valid (delegated authentication)."""
        config = OnedriveAccessConfig(oauth_token="ey.access.token")
        assert config.oauth_token == "ey.access.token"
        assert config.client_cred is None

    def test_no_auth_raises_error(self):
        """No authentication provided should raise ValueError."""
        with pytest.raises(ValueError, match="must be set"):
            OnedriveAccessConfig()

    def test_oauth_and_client_cred_raises_error(self):
        """Both oauth_token and client_cred provided should raise ValueError."""
        with pytest.raises(ValueError, match="cannot use both"):
            OnedriveAccessConfig(
                client_cred="secret-value",
                oauth_token="ey.access.token",
            )

    def test_oauth_and_password_raises_error(self):
        """oauth_token combined with password should raise ValueError."""
        with pytest.raises(ValueError, match="cannot use both"):
            OnedriveAccessConfig(
                password="user-password",
                oauth_token="ey.access.token",
            )
