import pytest
from pydantic import Secret

from unstructured_ingest.error import ValueError
from unstructured_ingest.processes.connectors.outlook import (
    OutlookAccessConfig,
    OutlookConnectionConfig,
    _prefer_immutable_ids,
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

    def test_empty_oauth_token_treated_as_missing(self):
        """An empty-string oauth_token (e.g. unset env var) should not satisfy the auth requirement.

        Validator and runtime both use truthiness; this test pins that consistency.
        """
        with pytest.raises(ValueError, match="must be set"):
            OutlookAccessConfig(oauth_token="")


class TestOutlookConnectionConfig:
    """Tests for OutlookConnectionConfig cross-field auth validation."""

    def test_client_cred_without_client_id_raises(self):
        """client_cred-based auth requires client_id; rejecting at config time
        avoids cryptic AADSTS / MSAL errors at runtime."""
        with pytest.raises(ValueError, match="client_id is required"):
            OutlookConnectionConfig(
                access_config=Secret(OutlookAccessConfig(client_cred="secret-value")),
            )

    def test_oauth_token_without_client_id_succeeds(self):
        """oauth_token auth doesn't need client_id; this is the delegated path."""
        config = OutlookConnectionConfig(
            access_config=Secret(OutlookAccessConfig(oauth_token="ey.access.token")),
        )
        assert config.client_id is None


class TestPreferImmutableIdsHeader:
    """`Prefer: IdType="ImmutableId"` keeps message ids stable across folder moves.

    Without it, Outlook/Exchange can rotate a message's id when the message is
    moved between folders, breaking downstream record identity that keys off
    FileData.identifier.
    """

    def test_hook_sets_header_on_request(self):
        try:
            from office365.runtime.http.request_options import RequestOptions
        except ImportError:
            pytest.skip("office365-rest-python-client not installed")

        request = RequestOptions("https://graph.microsoft.com/v1.0/me/messages")
        _prefer_immutable_ids(request)

        assert request.headers["Prefer"] == 'IdType="ImmutableId"'

    def test_get_client_registers_hook_that_fires_on_every_request(self):
        # Fires the SDK's own dispatch path directly (ClientRuntimeContext.build_request
        # calls exactly this: pending_request().beforeExecute.notify(request)) rather than
        # asserting on a mocked call signature, so e.g. a rename of the `once` kwarg would
        # be caught here instead of silently passing an unspecced mock assertion.
        try:
            from office365.runtime.http.request_options import RequestOptions
        except ImportError:
            pytest.skip("office365-rest-python-client not installed")

        config = OutlookConnectionConfig(
            access_config=Secret(OutlookAccessConfig(oauth_token="ey.access.token")),
        )
        client = config.get_client()

        initial_request = RequestOptions(
            "https://graph.microsoft.com/v1.0/users/alice/mailFolders/inbox/messages"
        )
        client.pending_request().beforeExecute.notify(initial_request)
        assert initial_request.headers["Prefer"] == 'IdType="ImmutableId"'

        # The hook must still be registered on the same pending_request() for a
        # get_all() pagination continuation, not just the first request.
        continuation_request = RequestOptions(
            "https://graph.microsoft.com/v1.0/users/alice/mailFolders/inbox/messages"
            "?$skiptoken=abc123"
        )
        client.pending_request().beforeExecute.notify(continuation_request)
        assert continuation_request.headers["Prefer"] == 'IdType="ImmutableId"'
