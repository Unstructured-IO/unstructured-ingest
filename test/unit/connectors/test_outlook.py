import email
import email.policy
import hashlib
import logging
import re
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, Mock, patch

import pytest
from pydantic import Secret

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import ValueError
from unstructured_ingest.processes.connectors.outlook import (
    BODY_PART_PREFERENCE,
    MESSAGE_SELECT_FIELDS,
    MESSAGES_PAGE_SIZE,
    UNIQUE_BODY_FIELD,
    OutlookAccessConfig,
    OutlookConnectionConfig,
    OutlookDownloader,
    OutlookDownloaderConfig,
    OutlookIndexer,
    OutlookIndexerConfig,
    _carries_text,
    _prefer_body_rendering,
    _prefer_immutable_ids,
    _primary_body_part,
    _reduce_message_body,
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


class TestMessageToFileDataIdentity:
    """The message id must reach every identity field unmodified.

    FileData.identifier keys incremental record identity downstream, the
    record_locator's message_id is how the downloader re-fetches the message,
    and the download filename is derived from the id. Any normalization,
    hashing, or re-derivation of the id here silently re-keys entire
    mailboxes, which is exactly the failure mode immutable ids exist to
    prevent.
    """

    def test_identity_fields_pass_through_message_id(self):
        indexer = _make_indexer()
        message = _make_message(message_id="msg-identity-1")

        file_data = indexer._message_to_file_data(message)

        assert file_data.identifier == "msg-identity-1"
        assert file_data.metadata.record_locator["message_id"] == "msg-identity-1"
        expected_name = hashlib.sha256(b"msg-identity-1").hexdigest()[:16] + ".eml"
        assert file_data.source_identifiers.fullpath == expected_name
        assert file_data.source_identifiers.filename == expected_name


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


def _make_folder(folder_id: str = "folder-1") -> Mock:
    folder = Mock()
    folder.id = folder_id
    # select(...) returns self on the real SDK, so the mock must too for the
    # .select(...).get_all(...) chain in _list_messages to resolve.
    folder.messages.select.return_value = folder.messages
    return folder


class TestListMessagesPagination:
    """get_all() follows @odata.nextLink; the old `.get().top(MAX)` call silently
    truncated enumeration at one page for large folders/mailboxes."""

    def test_uses_get_all_with_bounded_page_size(self):
        indexer = _make_indexer(recursive=False)
        message = Mock()
        root_folder = _make_folder("root")
        root_folder.messages.get_all.return_value.execute_query.return_value = [message]

        with patch.object(OutlookIndexer, "_get_selected_root_folders", return_value=[root_folder]):
            messages = indexer._list_messages(recursive=False)

        root_folder.messages.select.assert_called_once_with(MESSAGE_SELECT_FIELDS)
        root_folder.messages.get_all.assert_called_once_with(page_size=MESSAGES_PAGE_SIZE)
        root_folder.messages.get.assert_not_called()
        assert messages == [message]

    def test_select_projection_names_every_field_the_connector_reads(self):
        # Pinned as literals: _message_to_file_data and the metadata mapping
        # read exactly these Graph fields, and a projection that drifts
        # narrower silently nulls whatever it drops.
        assert MESSAGE_SELECT_FIELDS == [
            "id",
            "changeKey",
            "lastModifiedDateTime",
            "createdDateTime",
            "from",
            "toRecipients",
            "subject",
            "conversationId",
            "isDraft",
            "isRead",
            "hasAttachments",
            "importance",
        ]

    def test_accumulates_every_record_across_page_boundaries(self):
        # get_all() drains @odata.nextLink continuations into one collection;
        # a listing larger than one Graph page must come back complete and in
        # order, not truncated at page_size.
        indexer = _make_indexer(recursive=False)
        spanning_three_pages = [Mock() for _ in range(MESSAGES_PAGE_SIZE * 2 + 3)]
        root_folder = _make_folder("root")
        root_folder.messages.get_all.return_value.execute_query.return_value = spanning_three_pages

        with patch.object(OutlookIndexer, "_get_selected_root_folders", return_value=[root_folder]):
            messages = indexer._list_messages(recursive=False)

        assert messages == spanning_three_pages

    def test_recursion_pages_child_folders_via_get_all(self):
        indexer = _make_indexer(recursive=True)

        child_message = Mock()
        child_folder = _make_folder("child")
        child_folder.messages.get_all.return_value.execute_query.return_value = [child_message]
        child_folder.child_folders.get_all.return_value.execute_query.return_value = []

        root_message = Mock()
        root_folder = _make_folder("root")
        root_folder.messages.get_all.return_value.execute_query.return_value = [root_message]
        root_folder.child_folders.get_all.return_value.execute_query.return_value = [child_folder]

        with patch.object(OutlookIndexer, "_get_selected_root_folders", return_value=[root_folder]):
            messages = indexer._list_messages(recursive=True)

        root_folder.child_folders.get_all.assert_called_once_with()
        root_folder.child_folders.get.assert_not_called()
        assert messages == [root_message, child_message]

    def test_cycle_in_child_folder_graph_terminates(self):
        # get_all() makes each spin of a folder-graph cycle far more expensive
        # than the old single-page .get() (full pagination for messages and
        # child folders on every repeat visit); the visited-id guard must still
        # make this terminate rather than loop forever.
        indexer = _make_indexer(recursive=True)

        folder_a = _make_folder("folder-a")
        message_a = Mock()
        folder_a.messages.get_all.return_value.execute_query.return_value = [message_a]

        folder_b = _make_folder("folder-b")
        message_b = Mock()
        folder_b.messages.get_all.return_value.execute_query.return_value = [message_b]

        # Each folder's child_folders points back at the other: a 2-cycle.
        folder_a.child_folders.get_all.return_value.execute_query.return_value = [folder_b]
        folder_b.child_folders.get_all.return_value.execute_query.return_value = [folder_a]

        with patch.object(OutlookIndexer, "_get_selected_root_folders", return_value=[folder_a]):
            messages = indexer._list_messages(recursive=True)

        # Each folder was expanded exactly once, not once per cycle repetition.
        folder_a.messages.select.assert_called_once_with(MESSAGE_SELECT_FIELDS)
        folder_b.messages.select.assert_called_once_with(MESSAGE_SELECT_FIELDS)
        folder_a.messages.get_all.assert_called_once_with(page_size=MESSAGES_PAGE_SIZE)
        folder_b.messages.get_all.assert_called_once_with(page_size=MESSAGES_PAGE_SIZE)
        folder_a.child_folders.get_all.assert_called_once_with()
        folder_b.child_folders.get_all.assert_called_once_with()
        assert messages == [message_a, message_b]


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


class TestGetSelectedRootFoldersMatching:
    """Coverage for the display_name matching in _get_selected_root_folders,
    independent of the get_all() pagination mechanics covered above."""

    def _with_folders(self, indexer, folders):
        client_user = MagicMock()
        client_user.mail_folders.get_all.return_value.execute_query.return_value = folders
        client = MagicMock()
        client.users.__getitem__.return_value = client_user
        indexer.connection_config.get_client.return_value = client

    def test_folder_name_matching_is_case_insensitive(self):
        indexer = _make_indexer(outlook_folders=["INBOX"])
        folder = Mock()
        folder.display_name = "Inbox"
        self._with_folders(indexer, [folder])

        result = indexer._get_selected_root_folders()

        assert result == [folder]

    def test_no_matching_folder_raises_value_error(self):
        indexer = _make_indexer(outlook_folders=["Nonexistent Folder"])
        folder = Mock()
        folder.display_name = "Inbox"
        self._with_folders(indexer, [folder])

        with pytest.raises(ValueError, match="Root folders selected in configuration not found"):
            indexer._get_selected_root_folders()


class TestMessagesPageSizeConstant:
    def test_within_graph_top_limits(self):
        # Graph documents $top as 1..1000 for /messages; MESSAGES_PAGE_SIZE is a
        # plain module constant, so nothing else pins it to a sane value (a test
        # that only asserts get_all() was called with page_size=MESSAGES_PAGE_SIZE
        # would pass even if the constant itself were 0 or 100_000).
        assert 1 <= MESSAGES_PAGE_SIZE <= 1000


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


EML_DIR = Path(__file__).resolve().parents[3] / "example-docs" / "eml"

# Committed fixtures chosen for MIME structure rather than content.
HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT = EML_DIR / "email-with-image.eml"
HTML_AND_PLAIN_WITH_TEXT_ATTACHMENT = EML_DIR / "fake-email-attachment.eml"
# Carries two attachments and a long non-ASCII Authentication-Results header,
# which is the only shape that catches header refolding on re-serialization.
LONG_NON_ASCII_HEADERS = EML_DIR / "email-no-utf8-2014-03-17.111517.eml"
# multipart/related: the inline image is a sibling of the body part.
INLINE_IMAGE_RELATED = EML_DIR / "fake-email-image-embedded.eml"

STRUCTURED_FIXTURES = [
    HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT,
    HTML_AND_PLAIN_WITH_TEXT_ATTACHMENT,
    LONG_NON_ASCII_HEADERS,
    INLINE_IMAGE_RELATED,
]

UNIQUE_HTML = "<html><body><p>Only the newest sentence.</p></body></html>"

PREFER_BODY_RENDERING = "unstructured_ingest.processes.connectors.outlook._prefer_body_rendering"

# Must survive inside an attached message rather than being mistaken for a
# superseded rendering of the outer body.
NESTED_SENTINEL = "THE ATTACHED MESSAGE BODY MUST SURVIVE"

SIGNED_MESSAGE = (
    b'Content-Type: multipart/signed; protocol="application/pkcs7-signature"; '
    b'micalg=sha-256; boundary="s1"\r\nSubject: signed mail\r\n\r\n'
    b"--s1\r\nContent-Type: text/plain\r\n\r\nThe signed body text.\r\n"
    b"--s1\r\nContent-Type: application/pkcs7-signature\r\n\r\nc2lnbmF0dXJl\r\n--s1--\r\n"
)

# A single-part message: the body part is the message itself, so the surgery
# rewrites top-level headers rather than a child part's.
SINGLE_PART_PLAIN = (
    b"Subject: quarterly update\r\nFrom: sender@example.com\r\nTo: rec@example.com\r\n"
    b"Content-Type: text/plain; charset=us-ascii\r\n\r\nThe original body text.\r\n"
)

ATTACHMENT_ONLY = (
    b'Content-Type: multipart/mixed; boundary="b1"\r\nSubject: attachment only\r\n\r\n'
    b"--b1\r\nContent-Type: application/pdf\r\n"
    b'Content-Disposition: attachment; filename="a.pdf"\r\n\r\nnot-a-pdf\r\n--b1--\r\n'
)

# An empty body plus an empty uniqueBody: the shape Graph returns for a message
# that carries only attachments in its body slot.
EMPTY_BODY = (
    b"Subject: nothing to say\r\nFrom: sender@example.com\r\n"
    b"Content-Type: text/plain; charset=us-ascii\r\n\r\n\r\n"
)


def _parse(raw: bytes):
    return email.message_from_bytes(raw, policy=email.policy.default)


def _leaves(message):
    return [part for part in message.walk() if not part.is_multipart()]


def _looks_like_body(part) -> bool:
    """Body rendering rather than an attachment, judged from the wire only."""
    return (
        part.get_content_type() in ("text/plain", "text/html")
        and part.get_content_disposition() != "attachment"
        and part.get_filename() is None
    )


def _non_body_facts(message) -> list[tuple]:
    """Identity of every part that is not a body rendering."""
    return [
        (
            part.get_content_type(),
            part.get_filename(),
            part.get("Content-Transfer-Encoding"),
            part.get_payload(decode=True),
        )
        for part in _leaves(message)
        if not _looks_like_body(part)
    ]


def _header_facts(message) -> list[tuple[str, str]]:
    """Headers a body replacement has no business changing."""
    structural = {"content-type", "content-transfer-encoding", "mime-version"}
    return sorted(
        (name.lower(), str(value))
        for name, value in message.items()
        if name.lower() not in structural
    )


class TestPrimaryBodyPart:
    """The part reported must be the one a partitioner will read.

    Its subtype decides which rendering Graph is asked for, so a wrong answer
    here puts the wrong markup into the message.
    """

    @pytest.mark.parametrize("fixture", STRUCTURED_FIXTURES, ids=lambda p: p.name)
    def test_prefers_html_when_the_message_carries_both(self, fixture: Path):
        part = _primary_body_part(fixture.read_bytes())
        assert part is not None
        assert part.get_content_subtype() == "html"

    def test_reports_plain_when_there_is_no_html_rendering(self):
        part = _primary_body_part(SINGLE_PART_PLAIN)
        assert part is not None
        assert part.get_content_subtype() == "plain"

    def test_reports_none_when_there_is_no_body_part(self):
        assert _primary_body_part(ATTACHMENT_ONLY) is None

    def test_excludes_a_named_inline_html_part(self):
        raw = (
            b'Content-Type: multipart/mixed; boundary="mix"\r\n\r\n'
            b"--mix\r\nContent-Type: text/plain\r\n\r\nThe actual message body.\r\n"
            b"--mix\r\nContent-Type: text/html\r\n"
            b'Content-Disposition: inline; filename="report.html"\r\n\r\n'
            b"<p>Attached report contents.</p>\r\n--mix--\r\n"
        )
        assert _parse(raw).get_body(preferencelist=BODY_PART_PREFERENCE).get_filename() == (
            "report.html"
        )

        body = _primary_body_part(raw)

        assert body is not None
        assert body.get_content_type() == "text/plain"
        assert "actual message body" in body.get_content()


class TestReduceMessageBody:
    """The surgery must change the body and nothing else.

    Losing an attachment here would be worse than the duplication this
    feature exists to remove, so every invariant gets its own assertion.
    """

    @pytest.mark.parametrize("fixture", STRUCTURED_FIXTURES, ids=lambda p: p.name)
    def test_non_body_parts_are_untouched(self, fixture: Path):
        raw = fixture.read_bytes()
        before = _non_body_facts(_parse(raw))

        reduced = _reduce_message_body(raw, UNIQUE_HTML)

        assert reduced is not None
        assert _non_body_facts(_parse(reduced)) == before

    def test_a_single_part_message_keeps_its_identifying_headers(self):
        """The body part is the message itself here, so the surgery rewrites the
        top-level content type and transfer encoding by design. Everything that
        identifies the message still has to survive."""
        reduced = _reduce_message_body(SINGLE_PART_PLAIN, "Only the newest sentence.")

        assert reduced is not None
        rebuilt, original = _parse(reduced), _parse(SINGLE_PART_PLAIN)
        for header in ("Subject", "From", "To"):
            assert str(rebuilt[header]) == str(original[header])
        assert "Only the newest sentence." in rebuilt.get_content()
        assert "The original body text." not in rebuilt.get_content()

    @pytest.mark.parametrize("fixture", STRUCTURED_FIXTURES, ids=lambda p: p.name)
    def test_headers_are_untouched(self, fixture: Path):
        raw = fixture.read_bytes()
        before = _header_facts(_parse(raw))

        reduced = _reduce_message_body(raw, UNIQUE_HTML)

        assert _header_facts(_parse(reduced)) == before

    def test_long_non_ascii_headers_are_not_refolded(self):
        """The default serialization policy rewrites long source headers.

        Pinned separately from the parametrized case because a fixture with
        only short headers cannot catch it: the failure is whitespace inserted
        inside a header the replacement never touched.
        """
        raw = LONG_NON_ASCII_HEADERS.read_bytes()
        original = _parse(raw)
        long_headers = [name for name, value in original.items() if len(f"{name}: {value}") > 200]
        assert long_headers, "fixture no longer carries a long header to protect"

        reduced = _reduce_message_body(raw, UNIQUE_HTML)

        rebuilt = _parse(reduced)
        for name in long_headers:
            assert str(rebuilt[name]) == str(original[name])

    @pytest.mark.parametrize("fixture", STRUCTURED_FIXTURES, ids=lambda p: p.name)
    def test_the_body_part_carries_the_supplied_text(self, fixture: Path):
        reduced = _reduce_message_body(fixture.read_bytes(), UNIQUE_HTML)

        body = _parse(reduced).get_body(preferencelist=BODY_PART_PREFERENCE)
        assert "Only the newest sentence." in body.get_content()

    @pytest.mark.parametrize("fixture", STRUCTURED_FIXTURES, ids=lambda p: p.name)
    def test_no_other_body_rendering_survives(self, fixture: Path):
        """A stale plain-text rendering would still hold the quoted history.

        The partitioner accepts a setting that flips its preference to plain
        text, which would silently restore that history and make the whole
        feature a no-op, so the other renderings are removed rather than left.
        """
        raw = fixture.read_bytes()
        assert len([p for p in _leaves(_parse(raw)) if _looks_like_body(p)]) > 1

        reduced = _reduce_message_body(raw, UNIQUE_HTML)

        assert len([p for p in _leaves(_parse(reduced)) if _looks_like_body(p)]) == 1

    @pytest.mark.parametrize("fixture", STRUCTURED_FIXTURES, ids=lambda p: p.name)
    def test_the_original_body_text_is_gone(self, fixture: Path):
        raw = fixture.read_bytes()
        original_body = _parse(raw).get_body(preferencelist=BODY_PART_PREFERENCE)
        # Markup is stripped rather than used to skip lines: skipping any line
        # containing a tag leaves nothing to assert on an HTML body.
        words = [
            word
            for word in re.sub(r"<[^>]+>", " ", original_body.get_content()).split()
            if len(word) > 8 and word.isalpha()
        ]
        assert words, "fixture body has no distinctive words to check"

        reduced = _reduce_message_body(raw, UNIQUE_HTML)

        rebuilt_text = _parse(reduced).get_body(preferencelist=BODY_PART_PREFERENCE).get_content()
        for word in words:
            assert word not in rebuilt_text

    def test_a_named_inline_html_part_is_not_replaced_or_removed(self):
        raw = (
            b'Content-Type: multipart/mixed; boundary="mix"\r\n\r\n'
            b"--mix\r\nContent-Type: text/plain\r\n\r\nThe actual message body.\r\n"
            b"--mix\r\nContent-Type: text/html\r\n"
            b'Content-Disposition: inline; filename="report.html"\r\n\r\n'
            b"<p>Attached report contents.</p>\r\n--mix--\r\n"
        )

        reduced = _reduce_message_body(raw, "Only the newest sentence.")

        assert reduced is not None
        rebuilt = _parse(reduced)
        named = next(part for part in rebuilt.walk() if part.get_filename() == "report.html")
        assert named.get_content_disposition() == "inline"
        assert "Attached report contents." in named.get_content()
        body = _primary_body_part(reduced)
        assert body is not None
        assert "Only the newest sentence." in body.get_content()

    def test_preserves_the_content_id_of_a_related_root(self):
        raw = (
            b'Content-Type: multipart/related; boundary="rel"; start="<root>"\r\n\r\n'
            b"--rel\r\nContent-Type: image/png\r\nContent-ID: <image>\r\n\r\n"
            b"cG5n\r\n"
            b"--rel\r\nContent-Type: text/html\r\nContent-ID: <root>\r\n\r\n"
            b"<p>The full body and quoted history.</p>\r\n--rel--\r\n"
        )
        original_body = _parse(raw).get_body(preferencelist=BODY_PART_PREFERENCE)
        assert original_body is not None
        assert original_body["Content-ID"] == "<root>"

        reduced = _reduce_message_body(raw, UNIQUE_HTML)

        assert reduced is not None
        rebuilt = _parse(reduced)
        body = rebuilt.get_body(preferencelist=BODY_PART_PREFERENCE)
        assert body is not None
        assert body["Content-ID"] == "<root>"
        assert "Only the newest sentence." in body.get_content()


class TestReduceMessageBodyLeavesNestedMessagesAlone:
    """An attached message has a body of its own, and it must survive.

    Forwarding mail as an attachment is ordinary Outlook behaviour, and the
    attached message's body carries no attachment disposition and no filename,
    so it looks exactly like a superseded rendering of the outer body. Removing
    body parts anywhere but the outer body's own container guts the attachment.
    """

    def _message_with_an_attached_message(self) -> bytes:
        outer = EmailMessage()
        outer["Subject"] = "please see the attached mail"
        outer["From"] = "sender@example.com"
        outer.set_content("outer plain body")
        outer.add_alternative("<p>outer html body</p>", subtype="html")

        attached = EmailMessage()
        attached["Subject"] = "the forwarded message"
        attached["From"] = "original@example.com"
        attached.set_content(NESTED_SENTINEL)

        # add_attachment of a message object yields a message/rfc822 part,
        # which is the shape Outlook produces for mail forwarded as a file.
        outer.add_attachment(attached)
        assert any(
            part.get_content_type() == "message/rfc822" for part in _parse(outer.as_bytes()).walk()
        )
        return outer.as_bytes()

    def test_the_attached_message_body_survives(self):
        raw = self._message_with_an_attached_message()

        reduced = _reduce_message_body(raw, UNIQUE_HTML)

        assert reduced is not None
        assert NESTED_SENTINEL in reduced.decode("utf-8", "replace")

    def test_only_the_outer_bodys_own_rendering_is_removed(self):
        raw = self._message_with_an_attached_message()

        reduced = _reduce_message_body(raw, UNIQUE_HTML)

        before = len(_leaves(_parse(raw)))
        after = len(_leaves(_parse(reduced)))
        assert after == before - 1

    def test_the_outer_body_is_still_replaced(self):
        raw = self._message_with_an_attached_message()

        reduced = _reduce_message_body(raw, UNIQUE_HTML)

        body = _parse(reduced).get_body(preferencelist=BODY_PART_PREFERENCE)
        assert "Only the newest sentence." in body.get_content()
        assert "outer plain body" not in reduced.decode("utf-8", "replace")


class TestReduceMessageBodySweepsNestedContainers:
    """The superseded rendering can sit in a different container.

    A message whose HTML body lives in a related group alongside its images,
    with the plain-text rendering in a sibling alternative group, is ordinary
    mail. Sweeping only the body part's own container would leave that plain
    rendering, quoted history and all, for a plain-preferring partitioner to
    read, and the setting would silently do nothing.
    """

    def _nested(self) -> bytes:
        raw = (
            b'Content-Type: multipart/mixed; boundary="mix"\r\n'
            b"Subject: nested renderings\r\n\r\n"
            b'--mix\r\nContent-Type: multipart/alternative; boundary="alt"\r\n\r\n'
            b"--alt\r\nContent-Type: text/plain\r\n\r\n"
            b"PLAINRENDERING with the whole quoted history.\r\n"
            b"--alt--\r\n"
            b'--mix\r\nContent-Type: multipart/related; boundary="rel"\r\n\r\n'
            b"--rel\r\nContent-Type: text/html\r\n\r\n"
            b"<p>HTMLRENDERING with the whole quoted history.</p>\r\n"
            b"--rel\r\nContent-Type: image/png\r\nContent-ID: <img1>\r\n\r\n"
            b"cG5nYnl0ZXM=\r\n--rel--\r\n--mix--\r\n"
        )
        assert _parse(raw).get_body(preferencelist=BODY_PART_PREFERENCE).get_content_type() == (
            "text/html"
        )
        return raw

    def test_the_plain_rendering_in_another_container_is_removed(self):
        reduced = _reduce_message_body(self._nested(), UNIQUE_HTML)

        assert reduced is not None
        assert b"PLAINRENDERING" not in reduced

    def test_the_inline_image_survives(self):
        reduced = _reduce_message_body(self._nested(), UNIQUE_HTML)

        types = [part.get_content_type() for part in _leaves(_parse(reduced))]
        assert "image/png" in types

    def test_no_empty_container_is_left_behind(self):
        """The alternative group holds nothing once its only rendering goes."""
        reduced = _reduce_message_body(self._nested(), UNIQUE_HTML)

        containers = [
            part
            for part in _parse(reduced).walk()
            if part.is_multipart() and not part.get_payload()
        ]
        assert containers == []


class TestReduceMessageBodyKeepsInlineImages:
    """An image pasted into a reply is carried as a related part, not as data.

    Outlook references it from the body as cid:<id>. The reference lives inside
    the body text being replaced, and the image itself is a sibling part, so
    both have to come through: the part because it is not a body rendering, and
    the reference because Graph keeps it in the value it returns.
    """

    def _body_with_a_reference(self) -> str:
        return '<html><body><p>Only the newest sentence.</p><img src="cid:img1"></body></html>'

    def test_the_referenced_image_part_survives(self):
        raw = INLINE_IMAGE_RELATED.read_bytes()

        reduced = _reduce_message_body(raw, self._body_with_a_reference())

        assert reduced is not None
        types = [part.get_content_type() for part in _leaves(_parse(reduced))]
        assert "image/png" in types

    def test_the_reference_itself_survives(self):
        raw = INLINE_IMAGE_RELATED.read_bytes()

        reduced = _reduce_message_body(raw, self._body_with_a_reference())

        body = _parse(reduced).get_body(preferencelist=BODY_PART_PREFERENCE)
        assert "cid:img1" in body.get_content()


class TestReduceMessageBodyLeavesAttachedContainersAlone:
    """A container carried as an attachment keeps its own text parts.

    An attached message is recognised by its content type, but a group saved as
    a file, for instance a related bundle, is only recognisable by its
    disposition or filename. Descending into one deletes the attachment's text.
    """

    def _message_with_an_attached_container(self) -> bytes:
        return (
            b'Content-Type: multipart/mixed; boundary="mix"\r\n'
            b"Subject: attached container\r\n\r\n"
            b'--mix\r\nContent-Type: multipart/alternative; boundary="alt"\r\n\r\n'
            b"--alt\r\nContent-Type: text/plain\r\n\r\nOuter plain rendering.\r\n"
            b"--alt\r\nContent-Type: text/html\r\n\r\n"
            b"<p>Outer html rendering.</p>\r\n--alt--\r\n"
            b'--mix\r\nContent-Type: multipart/related; boundary="rel"\r\n'
            b'Content-Disposition: attachment; filename="bundle.mhtml"\r\n\r\n'
            b"--rel\r\nContent-Type: text/html\r\n\r\n"
            b"<p>ATTACHEDBUNDLETEXT</p>\r\n"
            b"--rel\r\nContent-Type: image/png\r\nContent-ID: <b1>\r\n\r\n"
            b"cG5n\r\n--rel--\r\n--mix--\r\n"
        )

    def test_the_attached_containers_text_survives(self):
        raw = self._message_with_an_attached_container()

        reduced = _reduce_message_body(raw, UNIQUE_HTML)

        assert reduced is not None
        assert b"ATTACHEDBUNDLETEXT" in reduced

    def test_the_outer_plain_rendering_is_still_removed(self):
        raw = self._message_with_an_attached_container()

        reduced = _reduce_message_body(raw, UNIQUE_HTML)

        assert b"Outer plain rendering." not in reduced
        body = _parse(reduced).get_body(preferencelist=BODY_PART_PREFERENCE)
        assert "Only the newest sentence." in body.get_content()


class TestReduceMessageBodyLineEndings:
    def test_the_rebuilt_message_uses_crlf(self):
        """Graph delivers CRLF, and the default policy would flatten it."""
        reduced = _reduce_message_body(
            HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes(), UNIQUE_HTML
        )

        assert reduced is not None
        assert b"\r\n" in reduced
        assert reduced.replace(b"\r\n", b"").count(b"\n") == 0


class TestReduceMessageBodyDeclines:
    """Declining must be indistinguishable from the feature being off.

    Graph returns an empty uniqueBody for a legitimately empty body, so the
    condition cannot simply be "the value is empty".
    """

    @pytest.mark.parametrize(
        "unique_content",
        [None, "", "   \r\n  ", "<html><body><div></div></body></html>"],
        ids=["missing", "empty", "whitespace", "markup-without-text"],
    )
    def test_declines_when_the_value_carries_no_text(self, unique_content):
        raw = HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes()

        assert _reduce_message_body(raw, unique_content) is None

    def test_declines_when_there_is_no_body_part_to_replace(self):
        assert _reduce_message_body(ATTACHMENT_ONLY, UNIQUE_HTML) is None

    def test_declines_for_an_entity_only_value(self):
        """Non-breaking spaces are not text, and would blank a real body.

        The fixture has an HTML body, so the value is read as markup. Against a
        plain-text body the same string is literal text and is written, which
        TestCarriesText covers.
        """
        raw = HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes()

        assert _reduce_message_body(raw, "<p>&nbsp;&nbsp;</p>") is None

    def test_an_empty_body_and_an_empty_value_leave_the_message_alone(self):
        assert _reduce_message_body(EMPTY_BODY, "") is None


class TestPreferBodyRendering:
    """Graph names its renderings "text" and "html"; MIME says "plain".

    Passing a MIME subtype through unchanged makes Graph ignore the preference
    and answer with HTML, which then lands inside a plain-text part.
    """

    def _request(self):
        try:
            from office365.runtime.http.request_options import RequestOptions
        except ImportError:
            pytest.skip("office365-rest-python-client not installed")
        return RequestOptions("https://graph.microsoft.com/v1.0/users/alice/messages/m1")

    def test_translates_the_mime_subtype_for_a_plain_body(self):
        request = self._request()

        _prefer_body_rendering("plain")(request)

        assert request.headers["Prefer"] == 'outlook.body-content-type="text"'

    def test_passes_html_through(self):
        request = self._request()

        _prefer_body_rendering("html")(request)

        assert request.headers["Prefer"] == 'outlook.body-content-type="html"'

    def test_every_subtype_the_body_lookup_can_return_is_translatable(self):
        """The lookup can only ever yield these two, so neither may raise."""
        for mime_subtype in BODY_PART_PREFERENCE:
            _prefer_body_rendering(mime_subtype)(self._request())

    def test_composes_with_a_preference_already_set(self):
        """The immutable-id hook sets Prefer on the same client, so this appends."""
        request = self._request()

        _prefer_immutable_ids(request)
        _prefer_body_rendering("html")(request)

        assert request.headers["Prefer"] == 'IdType="ImmutableId", outlook.body-content-type="html"'


class TestUniqueBodyRawPropertyLookup:
    """Pins uniqueBody's shape on the pinned client, like changeKey above.

    The connector reads the raw property because 2.6.2 declares no typed
    accessor. If an upgrade adds one, this test says so rather than the
    connector silently reading None and keeping every full body.
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

    def test_there_is_no_typed_accessor(self):
        assert not hasattr(self._real_message(), "unique_body")

    def test_the_raw_property_holds_the_graph_payload(self):
        message = self._real_message()
        payload = {"contentType": "html", "content": "<p>new</p>"}

        message.set_property(UNIQUE_BODY_FIELD, payload)

        assert message.get_property(UNIQUE_BODY_FIELD) == payload


class TestOutlookDownloaderConfigDefault:
    def test_quoted_history_exclusion_is_off_by_default(self):
        assert OutlookDownloaderConfig().exclude_quoted_history is False


class TestCarriesText:
    """Angle brackets are markup in an HTML body and characters in a text one."""

    @pytest.mark.parametrize(
        "value",
        [None, "", "   ", "<div></div>", "<p>&nbsp;</p>", "<p>&#160;&#160;</p>"],
        ids=["none", "empty", "spaces", "tags", "nbsp-entity", "numeric-entity"],
    )
    def test_markup_without_words_is_not_text(self, value):
        assert _carries_text(value, is_markup=True) is False

    @pytest.mark.parametrize(
        "value",
        ["hello", "<p>hello</p>", "<p>&amp;</p>"],
        ids=["bare", "wrapped", "escaped-ampersand"],
    )
    def test_markup_with_words_is_text(self, value):
        assert _carries_text(value, is_markup=True) is True

    @pytest.mark.parametrize(
        "value",
        ["<no comment>", "<see attached>", "a < b and c > d"],
        ids=["bracketed-note", "bracketed-pointer", "comparison"],
    )
    def test_plain_text_in_angle_brackets_is_still_text(self, value):
        assert _carries_text(value, is_markup=False) is True

    @pytest.mark.parametrize(
        "value",
        ["<no comment>", "<see attached>"],
        ids=["bracketed-note", "bracketed-pointer"],
    )
    def test_the_same_values_read_as_empty_markup(self, value):
        """This is the miss the flag exists to prevent. Read as markup these
        strip to nothing, so a plain-text message would be left unreduced."""
        assert _carries_text(value, is_markup=True) is False

    @pytest.mark.parametrize("value", [None, "", "  \r\n "], ids=["none", "empty", "whitespace"])
    def test_plain_text_still_has_to_hold_something(self, value):
        assert _carries_text(value, is_markup=False) is False


class TestDownloaderQuotedHistoryRequest:
    """The extra Graph request happens only when the setting is on."""

    def _downloader(self, exclude_quoted_history: bool):
        connection_config = OutlookConnectionConfig(
            access_config=Secret(OutlookAccessConfig(oauth_token="ey.access.token")),
        )
        return OutlookDownloader(
            connection_config=connection_config,
            download_config=OutlookDownloaderConfig(exclude_quoted_history=exclude_quoted_history),
        )

    def _file_data(self):
        return FileData(
            identifier="msg-1",
            connector_type="outlook",
            source_identifiers=SourceIdentifiers(filename="msg-1.eml", fullpath="msg-1.eml"),
            metadata=FileDataSourceMetadata(
                record_locator={"message_id": "msg-1", "user_email": "alice@example.com"}
            ),
        )

    def _client_writing(self, raw: bytes, unique_content: Optional[str]):
        """A client whose download writes `raw` and whose select carries uniqueBody.

        The rendering it answers with matches the message's own body part, the
        way an honest service would, so a test that wants a mismatch has to say
        so explicitly. Returns the client and the message it hands out, so
        assertions name the message directly rather than re-walking the mock.
        """
        message = MagicMock()

        def _download(file_obj):
            file_obj.write(raw)
            return MagicMock()

        body = _primary_body_part(raw)
        rendering = "text" if body is not None and body.get_content_subtype() == "plain" else "html"

        message.download.side_effect = _download
        message.select.return_value = message
        # The pinned office365 client has no typed accessor for uniqueBody, so
        # the connector reads the raw property dict Graph sent.
        message.get_property.return_value = (
            {"contentType": rendering, "content": unique_content}
            if unique_content is not None
            else None
        )

        client = MagicMock()
        client.users.__getitem__.return_value.messages.__getitem__.return_value = message
        return client, message

    @pytest.mark.parametrize(
        "raw",
        [
            HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes(),
            HTML_AND_PLAIN_WITH_TEXT_ATTACHMENT.read_bytes(),
            LONG_NON_ASCII_HEADERS.read_bytes(),
            INLINE_IMAGE_RELATED.read_bytes(),
            SINGLE_PART_PLAIN,
            SIGNED_MESSAGE,
            EMPTY_BODY,
            ATTACHMENT_ONLY,
        ],
        ids=[
            "html-plain-image-attachment",
            "html-plain-text-attachment",
            "long-non-ascii-headers",
            "inline-image-related",
            "single-part-plain",
            "signed",
            "empty-body",
            "attachment-only",
        ],
    )
    def test_the_setting_off_changes_nothing_at_all(self, tmp_path: Path, raw: bytes):
        """The default must be indistinguishable from the connector before this.

        Byte-identical output and no extra request, on every message shape the
        suite knows about, is the whole no-breaking-change claim.
        """
        downloader = self._downloader(exclude_quoted_history=False)
        client, message = self._client_writing(raw, UNIQUE_HTML)
        download_path = tmp_path / "msg-1.eml"

        with patch.object(OutlookConnectionConfig, "get_client", return_value=client):
            downloader._download_message(self._file_data(), download_path)

        assert download_path.read_bytes() == raw
        message.select.assert_not_called()
        client.execute_query.assert_not_called()
        assert [path.name for path in tmp_path.iterdir()] == ["msg-1.eml"]

    def test_one_selected_request_when_the_setting_is_on(self, tmp_path: Path):
        raw = HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes()
        downloader = self._downloader(exclude_quoted_history=True)
        client, message = self._client_writing(raw, UNIQUE_HTML)
        download_path = tmp_path / "msg-1.eml"

        with (
            patch.object(OutlookConnectionConfig, "get_client", return_value=client),
            patch(PREFER_BODY_RENDERING) as prefer,
        ):
            downloader._download_message(self._file_data(), download_path)

        # An HTML body part must be matched by an HTML rendering request.
        prefer.assert_called_once_with("html")

        message.select.assert_called_once_with(["id", UNIQUE_BODY_FIELD])
        client.execute_query.assert_called_once()

    def test_the_written_file_carries_only_the_unique_text(self, tmp_path: Path):
        raw = HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes()
        downloader = self._downloader(exclude_quoted_history=True)
        client, _ = self._client_writing(raw, UNIQUE_HTML)
        download_path = tmp_path / "msg-1.eml"

        with patch.object(OutlookConnectionConfig, "get_client", return_value=client):
            downloader._download_message(self._file_data(), download_path)

        written = download_path.read_bytes()
        assert written != raw
        body = _parse(written).get_body(preferencelist=BODY_PART_PREFERENCE)
        assert "Only the newest sentence." in body.get_content()
        assert _non_body_facts(_parse(written)) == _non_body_facts(_parse(raw))

    def test_the_full_body_is_kept_when_graph_returns_nothing(self, tmp_path: Path):
        raw = HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes()
        downloader = self._downloader(exclude_quoted_history=True)
        client, _ = self._client_writing(raw, None)
        download_path = tmp_path / "msg-1.eml"

        with patch.object(OutlookConnectionConfig, "get_client", return_value=client):
            downloader._download_message(self._file_data(), download_path)

        assert download_path.read_bytes() == raw

    def test_one_client_serves_both_requests(self, tmp_path: Path):
        """A second client would mean a second token acquisition per message."""
        raw = HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes()
        downloader = self._downloader(exclude_quoted_history=True)
        client, _ = self._client_writing(raw, UNIQUE_HTML)
        download_path = tmp_path / "msg-1.eml"

        with patch.object(OutlookConnectionConfig, "get_client", return_value=client) as get_client:
            downloader._download_message(self._file_data(), download_path)

        get_client.assert_called_once()

    def test_a_lost_reduction_is_warned_about(self, tmp_path: Path, caplog):
        """A record keeping history it was meant to lose has to be visible."""
        raw = HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes()
        downloader = self._downloader(exclude_quoted_history=True)
        client, _ = self._client_writing(raw, None)
        download_path = tmp_path / "msg-1.eml"

        with (
            caplog.at_level(logging.WARNING),
            patch.object(OutlookConnectionConfig, "get_client", return_value=client),
        ):
            downloader._download_message(self._file_data(), download_path)

        assert any(record.levelno == logging.WARNING for record in caplog.records)

    def test_an_empty_body_is_not_warned_about(self, tmp_path: Path, caplog):
        """An attachment-only message has nothing to reduce, so a warning here
        would make a healthy mailbox look broken."""
        downloader = self._downloader(exclude_quoted_history=True)
        client, _ = self._client_writing(EMPTY_BODY, "")
        download_path = tmp_path / "msg-1.eml"

        with (
            caplog.at_level(logging.INFO),
            patch.object(OutlookConnectionConfig, "get_client", return_value=client),
        ):
            downloader._download_message(self._file_data(), download_path)

        assert download_path.read_bytes() == EMPTY_BODY
        assert not [record for record in caplog.records if record.levelno >= logging.WARNING]

    def test_a_plain_only_message_asks_for_the_plain_rendering(self, tmp_path: Path):
        """A plain body must not be answered with HTML.

        The MIME subtype here is "plain", which Graph does not accept; the
        translation to its own "text" token happens inside the helper this
        asserts on, and is covered by TestPreferBodyRendering.
        """
        downloader = self._downloader(exclude_quoted_history=True)
        client, _ = self._client_writing(SINGLE_PART_PLAIN, "Only the newest sentence.")
        download_path = tmp_path / "msg-1.eml"

        with (
            patch.object(OutlookConnectionConfig, "get_client", return_value=client),
            patch(PREFER_BODY_RENDERING) as prefer,
        ):
            downloader._download_message(self._file_data(), download_path)

        prefer.assert_called_once_with("plain")

        written = _parse(download_path.read_bytes())
        assert written.get_content_type() == "text/plain"
        assert "<" not in written.get_content()

    def test_a_failed_extra_request_does_not_fail_the_record(self, tmp_path: Path, caplog):
        """The full message is already on disk, so a throttled or failed extra
        request must leave a usable record rather than raise."""
        raw = HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes()
        downloader = self._downloader(exclude_quoted_history=True)
        client, message = self._client_writing(raw, UNIQUE_HTML)
        client.execute_query.side_effect = RuntimeError("429 too many requests")
        download_path = tmp_path / "msg-1.eml"

        with (
            caplog.at_level(logging.WARNING),
            patch.object(OutlookConnectionConfig, "get_client", return_value=client),
        ):
            downloader._download_message(self._file_data(), download_path)

        assert download_path.read_bytes() == raw
        assert any(record.levelno == logging.WARNING for record in caplog.records)

    def test_a_rendering_graph_did_not_honour_is_refused(self, tmp_path: Path, caplog):
        """Graph may ignore an unsupported preference and answer in its own
        default. Writing HTML into a plain-text part would have the partitioner
        extract literal markup, so the full body is kept instead."""
        downloader = self._downloader(exclude_quoted_history=True)
        client, message = self._client_writing(SINGLE_PART_PLAIN, "<p>html we did not ask for</p>")
        message.get_property.return_value = {
            "contentType": "html",
            "content": "<p>html we did not ask for</p>",
        }
        download_path = tmp_path / "msg-1.eml"

        with (
            caplog.at_level(logging.WARNING),
            patch.object(OutlookConnectionConfig, "get_client", return_value=client),
        ):
            downloader._download_message(self._file_data(), download_path)

        assert download_path.read_bytes() == SINGLE_PART_PLAIN
        assert any(record.levelno == logging.WARNING for record in caplog.records)

    def test_a_signed_message_is_left_alone(self, tmp_path: Path):
        """Replacing the body of a signed message would leave it claiming a
        signature it no longer satisfies, and no extra request is worth making."""
        downloader = self._downloader(exclude_quoted_history=True)
        client, message = self._client_writing(SIGNED_MESSAGE, "Only the newest sentence.")
        download_path = tmp_path / "msg-1.eml"

        with patch.object(OutlookConnectionConfig, "get_client", return_value=client):
            downloader._download_message(self._file_data(), download_path)

        assert download_path.read_bytes() == SIGNED_MESSAGE
        message.select.assert_not_called()

    def test_no_staging_file_is_left_behind(self, tmp_path: Path):
        """The reduced message is staged beside the target and moved into place,
        so a later run must not find a stray partial file to reuse."""
        raw = HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes()
        downloader = self._downloader(exclude_quoted_history=True)
        client, _ = self._client_writing(raw, UNIQUE_HTML)
        download_path = tmp_path / "msg-1.eml"

        with patch.object(OutlookConnectionConfig, "get_client", return_value=client):
            downloader._download_message(self._file_data(), download_path)

        assert [path.name for path in tmp_path.iterdir()] == ["msg-1.eml"]


class TestReduceMessageBodyLeavesProtectedMessagesAlone:
    def test_a_signed_message_declines(self):
        assert _reduce_message_body(SIGNED_MESSAGE, UNIQUE_HTML) is None

    def test_an_encrypted_message_declines(self):
        encrypted = SIGNED_MESSAGE.replace(b"multipart/signed", b"multipart/encrypted")

        assert _reduce_message_body(encrypted, UNIQUE_HTML) is None
