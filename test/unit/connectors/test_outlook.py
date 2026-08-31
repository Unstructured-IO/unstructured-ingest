from datetime import datetime, timezone
from typing import Optional
from unittest.mock import MagicMock, Mock, patch

import pytest
from pydantic import Secret

from unstructured_ingest.error import ValueError
from unstructured_ingest.processes.connectors.outlook import (
    MESSAGES_PAGE_SIZE,
    OutlookAccessConfig,
    OutlookConnectionConfig,
    OutlookIndexer,
    OutlookIndexerConfig,
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


def _make_message(message_id: str = "msg-1", change_key: Optional[str] = "ck-123") -> Mock:
    message = Mock()
    message.id = message_id
    message.resource_url = f"https://graph.microsoft.com/v1.0/me/messages/{message_id}"
    message.get_property.return_value = change_key
    fixed_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    message.last_modified_datetime = fixed_time
    message.created_datetime = fixed_time
    message.sent_from = "sender@example.com"
    message.to_recipients = []
    message.subject = "Test subject"
    message.conversation_id = "conv-1"
    message.is_draft = False
    message.is_read = True
    message.has_attachments = False
    message.importance = "normal"
    return message


def _make_indexer(
    outlook_folders=None, recursive: bool = False, user_email: str = "alice@example.com"
) -> OutlookIndexer:
    conn = Mock(spec=OutlookConnectionConfig)
    idx_config = Mock(spec=OutlookIndexerConfig)
    idx_config.outlook_folders = outlook_folders or ["Inbox"]
    idx_config.recursive = recursive
    idx_config.user_email = user_email
    return OutlookIndexer(connection_config=conn, index_config=idx_config)


class TestMessageToFileDataVersion:
    """Regression coverage for `FileData.metadata.version`.

    OutlookItem.change_key reads message.properties["ChangeKey"], but Graph's JSON
    response uses "changeKey" (lowercase c), so the typed accessor always returned
    None. `_message_to_file_data` now reads the raw property directly instead.
    """

    def test_version_uses_raw_changekey_property(self):
        indexer = _make_indexer()
        message = _make_message(change_key="server-changekey-abc123")

        file_data = indexer._message_to_file_data(message)

        message.get_property.assert_called_once_with("changeKey")
        assert file_data.metadata.version == "server-changekey-abc123"

    def test_version_is_none_when_changekey_absent(self):
        indexer = _make_indexer()
        message = _make_message(change_key=None)

        file_data = indexer._message_to_file_data(message)

        assert file_data.metadata.version is None

    def test_version_is_none_when_changekey_is_empty_string(self):
        # An empty string would otherwise compare equal to a stored empty-string
        # version and defeat the platform's unchanged-record skip.
        indexer = _make_indexer()
        message = _make_message(change_key="")

        file_data = indexer._message_to_file_data(message)

        assert file_data.metadata.version is None


class TestChangeKeyRawPropertyLookup:
    """Pins the office365-rest-python-client casing mismatch against the real SDK.

    OutlookItem.change_key does `self.properties.get("ChangeKey", None)`, but Graph
    sends "changeKey". get_property("changeKey") reads the raw key directly and
    sidesteps the broken typed accessor. Uses the real Message/GraphClient classes
    (no network calls triggered by construction or set_property) so a future SDK
    upgrade that fixes the casing would surface here, not just in outlook.py.
    """

    def _real_message(self):
        try:
            from office365.graph_client import GraphClient
            from office365.outlook.mail.messages.message import Message
            from office365.runtime.paths.resource_path import ResourcePath
        except ImportError:
            pytest.skip("office365-rest-python-client not installed")
        client = GraphClient(lambda: {"access_token": "x", "token_type": "Bearer"})
        return Message(client, ResourcePath("messages/abc"))

    def test_typed_accessor_is_broken_for_real_graph_casing(self):
        # If this ever returns the value instead of None, upstream fixed the casing
        # bug and get_property("changeKey") in outlook.py could revert to the typed
        # message.change_key accessor.
        message = self._real_message()
        message.set_property("changeKey", "server-changekey-abc123")
        assert message.change_key is None

    def test_get_property_reads_the_actual_graph_casing(self):
        message = self._real_message()
        message.set_property("changeKey", "server-changekey-abc123")
        assert message.get_property("changeKey") == "server-changekey-abc123"

    def test_get_property_defaults_to_none_when_absent(self):
        message = self._real_message()
        assert message.get_property("changeKey") is None


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

    def test_get_client_registers_hook_for_every_request(self):
        try:
            from office365.graph_client import GraphClient
        except ImportError:
            pytest.skip("office365-rest-python-client not installed")

        config = OutlookConnectionConfig(
            access_config=Secret(OutlookAccessConfig(oauth_token="ey.access.token")),
        )

        with patch.object(GraphClient, "before_execute") as mock_before_execute:
            config.get_client()

        # once=False: the header must ride every request, including the paginated
        # continuations get_all() issues for large folders/mailboxes.
        mock_before_execute.assert_called_once_with(_prefer_immutable_ids, once=False)


class TestListMessagesPagination:
    """get_all() follows @odata.nextLink; the old `.get().top(MAX)` call silently
    truncated enumeration at one page for large folders/mailboxes."""

    def test_uses_get_all_with_bounded_page_size(self):
        indexer = _make_indexer(recursive=False)
        message = Mock()
        root_folder = Mock()
        root_folder.messages.get_all.return_value.execute_query.return_value = [message]

        with patch.object(OutlookIndexer, "_get_selected_root_folders", return_value=[root_folder]):
            messages = indexer._list_messages(recursive=False)

        root_folder.messages.get_all.assert_called_once_with(page_size=MESSAGES_PAGE_SIZE)
        root_folder.messages.get.assert_not_called()
        assert messages == [message]

    def test_recursion_pages_child_folders_via_get_all(self):
        indexer = _make_indexer(recursive=True)

        child_message = Mock()
        child_folder = Mock()
        child_folder.messages.get_all.return_value.execute_query.return_value = [child_message]
        child_folder.child_folders.get_all.return_value.execute_query.return_value = []

        root_message = Mock()
        root_folder = Mock()
        root_folder.messages.get_all.return_value.execute_query.return_value = [root_message]
        root_folder.child_folders.get_all.return_value.execute_query.return_value = [child_folder]

        with patch.object(OutlookIndexer, "_get_selected_root_folders", return_value=[root_folder]):
            messages = indexer._list_messages(recursive=True)

        root_folder.child_folders.get_all.assert_called_once_with()
        root_folder.child_folders.get.assert_not_called()
        assert messages == [root_message, child_message]


class TestGetSelectedRootFoldersPagination:
    """mail_folders enumeration must also follow pagination: Graph defaults
    /mailFolders to 10 per page, so a mailbox with more top-level folders than
    that was silently truncated before get_all() was used here."""

    def test_root_folders_use_get_all(self):
        indexer = _make_indexer(outlook_folders=["Inbox"])
        folder = Mock()
        folder.display_name = "Inbox"

        client_user = MagicMock()
        client_user.mail_folders.get_all.return_value.execute_query.return_value = [folder]
        client = MagicMock()
        client.users.__getitem__.return_value = client_user
        indexer.connection_config.get_client.return_value = client

        result = indexer._get_selected_root_folders()

        client_user.mail_folders.get_all.assert_called_once_with()
        client_user.mail_folders.get.assert_not_called()
        assert result == [folder]
