import pytest

from unstructured_ingest.error import ValueError
from unstructured_ingest.processes.connectors.google_drive import (
    GoogleDriveAccessConfig,
)


class TestGoogleDriveAccessConfig:
    """Tests for GoogleDriveAccessConfig authentication validation."""

    def test_oauth_token_only(self):
        """OAuth token alone should be valid."""
        config = GoogleDriveAccessConfig(oauth_token="ya29.a0AfH6SMBxxxxxxxx")
        assert config.get_auth_type() == "oauth"
        assert config.oauth_token == "ya29.a0AfH6SMBxxxxxxxx"

    def test_service_account_key_only(self):
        """Service account key alone should be valid."""
        config = GoogleDriveAccessConfig(
            service_account_key={"type": "service_account", "project_id": "test"}
        )
        assert config.get_auth_type() == "service_account"

    def test_service_account_key_as_json_string(self):
        """Service account key as JSON string should be valid."""
        import json

        key_dict = {"type": "service_account", "project_id": "test"}
        config = GoogleDriveAccessConfig(service_account_key=json.dumps(key_dict))
        assert config.get_auth_type() == "service_account"
        assert config.service_account_key == key_dict

    def test_no_auth_raises_error(self):
        """No authentication provided should raise ValueError."""
        with pytest.raises(ValueError, match="Authentication required"):
            GoogleDriveAccessConfig()

    def test_both_oauth_and_service_account_raises_error(self):
        """Both auth methods provided should raise ValueError."""
        with pytest.raises(ValueError, match="Multiple authentication methods"):
            GoogleDriveAccessConfig(
                service_account_key={"type": "service_account"},
                oauth_token="ya29.a0AfH6SMBxxxxxxxx",
            )

    def test_both_oauth_and_service_account_path_raises_error(self, tmp_path):
        """OAuth token + service account path should raise ValueError."""
        # Create a temp file for service account key
        key_file = tmp_path / "credentials.json"
        key_file.write_text('{"type": "service_account", "project_id": "test"}')

        with pytest.raises(ValueError, match="Multiple authentication methods"):
            GoogleDriveAccessConfig(
                service_account_key_path=key_file,
                oauth_token="ya29.a0AfH6SMBxxxxxxxx",
            )

    def test_service_account_key_path_only(self, tmp_path):
        """Service account key path alone should be valid."""
        key_file = tmp_path / "credentials.json"
        key_file.write_text('{"type": "service_account", "project_id": "test"}')

        config = GoogleDriveAccessConfig(service_account_key_path=key_file)
        assert config.get_auth_type() == "service_account"

    def test_get_service_account_key_from_path(self, tmp_path):
        """get_service_account_key should load from file path."""
        key_data = {"type": "service_account", "project_id": "test", "private_key": "xxx"}
        key_file = tmp_path / "credentials.json"
        key_file.write_text('{"type": "service_account", "project_id": "test", "private_key": "xxx"}')

        config = GoogleDriveAccessConfig(service_account_key_path=key_file)
        result = config.get_service_account_key()
        assert result == key_data

    def test_get_service_account_key_from_dict(self):
        """get_service_account_key should return the dict directly."""
        key_data = {"type": "service_account", "project_id": "test"}
        config = GoogleDriveAccessConfig(service_account_key=key_data)
        result = config.get_service_account_key()
        assert result == key_data

    def test_service_account_key_and_path_same_value(self, tmp_path):
        """If both key and path are provided with same value, should succeed."""
        key_data = {"type": "service_account", "project_id": "test"}
        key_file = tmp_path / "credentials.json"
        key_file.write_text('{"type": "service_account", "project_id": "test"}')

        config = GoogleDriveAccessConfig(
            service_account_key=key_data,
            service_account_key_path=key_file,
        )
        result = config.get_service_account_key()
        assert result == key_data

    def test_service_account_key_and_path_different_value_raises(self, tmp_path):
        """If both key and path are provided with different values, should raise."""
        key_data = {"type": "service_account", "project_id": "test1"}
        key_file = tmp_path / "credentials.json"
        key_file.write_text('{"type": "service_account", "project_id": "test2"}')

        config = GoogleDriveAccessConfig(
            service_account_key=key_data,
            service_account_key_path=key_file,
        )
        with pytest.raises(ValueError, match="both provided and have different values"):
            config.get_service_account_key()


class TestGoogleDriveAccessConfigAuthType:
    """Tests for get_auth_type method."""

    def test_auth_type_oauth(self):
        """Should return 'oauth' when oauth_token is set."""
        config = GoogleDriveAccessConfig(oauth_token="token123")
        assert config.get_auth_type() == "oauth"

    def test_auth_type_service_account_from_key(self):
        """Should return 'service_account' when service_account_key is set."""
        config = GoogleDriveAccessConfig(
            service_account_key={"type": "service_account"}
        )
        assert config.get_auth_type() == "service_account"

    def test_auth_type_service_account_from_path(self, tmp_path):
        """Should return 'service_account' when service_account_key_path is set."""
        key_file = tmp_path / "credentials.json"
        key_file.write_text('{"type": "service_account"}')

        config = GoogleDriveAccessConfig(service_account_key_path=key_file)
        assert config.get_auth_type() == "service_account"
