import email
import email.policy
import logging
from contextlib import suppress
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
from pydantic import Secret

from test.unit.connectors.outlook_messages import (
    ATTACHMENT_ONLY,
    EMPTY_BODY,
    HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT,
    HTML_AND_PLAIN_WITH_TEXT_ATTACHMENT,
    INLINE_IMAGE_RELATED,
    LONG_NON_ASCII_HEADERS,
    SIGNED_MESSAGE,
    SINGLE_PART_PLAIN,
    UNIQUE_HTML,
    _non_body_facts,
)
from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.processes.connectors.outlook import (
    UNIQUE_BODY_FIELD,
    OutlookAccessConfig,
    OutlookConnectionConfig,
    OutlookDownloader,
    OutlookDownloaderConfig,
    _prefer_body_rendering,
    _prefer_immutable_ids,
    _replacement_from_graph,
)
from unstructured_ingest.processes.connectors.outlook_mime import (
    BODY_PART_PREFERENCE,
    BodyRendering,
    ReplacementBody,
)


def parse(raw: bytes):
    return email.message_from_bytes(raw, policy=email.policy.default)


PREFER_BODY_RENDERING = "unstructured_ingest.processes.connectors.outlook._prefer_body_rendering"


class TestUniqueBodyFromGraph:
    """Graph's answer arrives in whichever shape the installed client gives it.

    office365 2.x leaves a field it declares no accessor for as the raw JSON
    dict; 3.x deserialises uniqueBody into a typed ItemBody whose contentType is
    an enum rather than a string. Reading only one of those shapes makes the
    whole feature a silent no-op on the other, one warning per message, so both
    are pinned here rather than assumed.
    """

    class _ItemBody:
        """The shape office365 3.x deserialises uniqueBody into."""

        def __init__(self, content, content_type):
            self.content = content
            self.contentType = content_type

    class _BodyType:
        """The shape 3.x gives contentType: an enum whose value is the name."""

        def __init__(self, value):
            self.value = value

    def test_reads_the_raw_dict(self):
        answer = _replacement_from_graph({"contentType": "html", "content": "<p>new</p>"})

        assert answer == ReplacementBody(rendering=BodyRendering.HTML, content="<p>new</p>")

    def test_reads_a_typed_body(self):
        answer = _replacement_from_graph(self._ItemBody("only the new text", "text"))

        assert answer == ReplacementBody(rendering=BodyRendering.TEXT, content="only the new text")

    def test_reads_a_typed_body_whose_rendering_is_an_enum(self):
        answer = _replacement_from_graph(self._ItemBody("<p>new</p>", self._BodyType("html")))

        assert answer == ReplacementBody(rendering=BodyRendering.HTML, content="<p>new</p>")

    def test_reads_a_rendering_in_any_case(self):
        answer = _replacement_from_graph({"contentType": "HTML", "content": "<p>new</p>"})

        assert answer is not None and answer.rendering is BodyRendering.HTML

    @pytest.mark.parametrize(
        "content_type",
        [None, "", "richText", "rtf"],
        ids=["missing", "empty", "unknown", "unsupported"],
    )
    def test_a_rendering_graph_did_not_name_is_not_a_rendering(self, content_type):
        """An unnamed rendering must not read as the one that was asked for.

        A missing contentType used to short-circuit the honour check, which put
        markup into a plain-text part for the partitioner to extract literally.
        """
        answer = _replacement_from_graph({"contentType": content_type, "content": "<p>new</p>"})

        assert answer is not None and answer.rendering is None

    def test_a_rendering_with_no_key_at_all_is_not_a_rendering(self):
        answer = _replacement_from_graph({"content": "<p>new</p>"})

        assert answer is not None and answer.rendering is None

    @pytest.mark.parametrize(
        "content",
        [{}, {"k": "v"}, 123, ["a"], b"bytes"],
        ids=["empty-dict", "dict", "int", "list", "bytes"],
    )
    def test_a_content_that_is_not_text_is_declined(self, content):
        """Graph types content as a string; anything else is it breaking its own
        schema, which is a reason to keep the downloaded message rather than to
        raise past the downloader and fail the record."""
        assert _replacement_from_graph({"contentType": "html", "content": content}) is None

    @pytest.mark.parametrize("value", [None, "", 0, "a string"], ids=str)
    def test_an_answer_with_no_body_in_it_is_declined(self, value):
        assert _replacement_from_graph(value) is None

    def test_an_empty_body_is_read_rather_than_declined(self):
        """An empty uniqueBody is Graph's honest answer for an empty message."""
        answer = _replacement_from_graph({"contentType": "html", "content": ""})

        assert answer == ReplacementBody(rendering=BodyRendering.HTML, content="")

    def test_the_client_round_trips_a_graph_payload(self):
        """Drives the installed client rather than a stand-in, so an upgrade that
        changes how uniqueBody deserialises fails here rather than in production."""
        try:
            from office365.graph_client import GraphClient
            from office365.outlook.mail.messages.message import Message
            from office365.runtime.paths.resource_path import ResourcePath
        except ImportError:
            pytest.skip("office365-rest-python-client not installed")
        message = Message(
            GraphClient(lambda: {"access_token": "x", "token_type": "Bearer"}),
            ResourcePath("messages/abc"),
        )

        message.set_property(UNIQUE_BODY_FIELD, {"contentType": "html", "content": "<p>new</p>"})

        assert _replacement_from_graph(message.get_property(UNIQUE_BODY_FIELD)) == ReplacementBody(
            rendering=BodyRendering.HTML, content="<p>new</p>"
        )


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

        _prefer_body_rendering(BodyRendering.TEXT)(request)

        assert request.headers["Prefer"] == 'outlook.body-content-type="text"'

    def test_passes_html_through(self):
        request = self._request()

        _prefer_body_rendering(BodyRendering.HTML)(request)

        assert request.headers["Prefer"] == 'outlook.body-content-type="html"'

    def test_every_subtype_the_body_lookup_can_return_is_translatable(self):
        """Body selection can only ever yield these two, so neither may raise."""
        for rendering in BodyRendering:
            _prefer_body_rendering(rendering)(self._request())

    def test_composes_with_a_preference_already_set(self):
        """The immutable-id hook sets Prefer on the same client, so this appends."""
        request = self._request()

        _prefer_immutable_ids(request)
        _prefer_body_rendering(BodyRendering.HTML)(request)

        assert request.headers["Prefer"] == 'IdType="ImmutableId", outlook.body-content-type="html"'

    def test_the_preference_rides_only_the_request_it_was_added_for(self):
        """Registration and removal have to be driven on a real event handler.

        A mock accepts `+=` and `-=` silently, so it cannot show either half of
        this working: the handler removes by identity and raises when the hook
        is already gone, and a hook left behind would append a second
        preference to every later request on the same client. The read itself
        is allowed to fail here, because the removal is in a finally and the
        point is what the client carries before and after it.
        """
        try:
            from office365.graph_client import GraphClient
            from office365.runtime.http.request_options import RequestOptions
        except ImportError:
            pytest.skip("office365-rest-python-client not installed")

        client = GraphClient(lambda: {"access_token": "x", "token_type": "Bearer"})
        # What get_client registers, so the composition is the real one.
        client.pending_request().beforeExecute += _prefer_immutable_ids
        sent = []

        def _execute_query():
            request = RequestOptions("https://graph.microsoft.com/v1.0/users/alice/messages/m1")
            client.pending_request().beforeExecute.notify(request)
            sent.append(request.headers.get("Prefer"))

        client.execute_query = _execute_query
        downloader = OutlookDownloader(
            connection_config=OutlookConnectionConfig(
                access_config=Secret(OutlookAccessConfig(oauth_token="ey.access.token"))
            ),
            download_config=OutlookDownloaderConfig(exclude_quoted_history=True),
        )

        with suppress(Exception):
            downloader._fetch_unique_body(client, "alice@example.com", "m1", BodyRendering.HTML)

        assert sent == ['IdType="ImmutableId", outlook.body-content-type="html"']

        later = RequestOptions("https://graph.microsoft.com/v1.0/users/alice/messages/m2")
        client.pending_request().beforeExecute.notify(later)
        assert later.headers["Prefer"] == 'IdType="ImmutableId"'


class TestOutlookDownloaderConfigDefault:
    def test_quoted_history_exclusion_is_off_by_default(self):
        assert OutlookDownloaderConfig().exclude_quoted_history is False


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

        body = email.message_from_bytes(raw, policy=email.policy.default).get_body(
            preferencelist=BODY_PART_PREFERENCE
        )
        rendering = "text" if body is not None and body.get_content_subtype() == "plain" else "html"

        message.download.side_effect = _download
        message.select.return_value = message
        # office365 2.x has no typed accessor for uniqueBody, so
        # the connector reads the raw property dict Graph sent; from_graph also
        # reads the typed shape 3.x returns, which TestUniqueBodyFromGraph pins.
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
        prefer.assert_called_once_with(BodyRendering.HTML)

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
        body = parse(written).get_body(preferencelist=BODY_PART_PREFERENCE)
        assert "Only the newest sentence." in body.get_content()
        assert _non_body_facts(parse(written)) == _non_body_facts(parse(raw))

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

        prefer.assert_called_once_with(BodyRendering.TEXT)

        written = parse(download_path.read_bytes())
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

    @pytest.mark.parametrize(
        "payload",
        [
            {"content": "<p>html we did not ask for</p>"},
            {"contentType": None, "content": "<p>html we did not ask for</p>"},
            {"contentType": "", "content": "<p>html we did not ask for</p>"},
        ],
        ids=["no-key", "null", "empty"],
    )
    def test_a_rendering_graph_did_not_name_is_refused_too(
        self, tmp_path: Path, caplog, payload: dict
    ):
        """An answer that names no rendering is no more honoured than a wrong one.

        This is the same failure as the test above reached by a different route:
        an unnamed rendering that reads as "close enough" writes markup into a
        plain-text part just as surely as an unhonoured one does.
        """
        downloader = self._downloader(exclude_quoted_history=True)
        client, message = self._client_writing(SINGLE_PART_PLAIN, "<p>html we did not ask for</p>")
        message.get_property.return_value = payload
        download_path = tmp_path / "msg-1.eml"

        with (
            caplog.at_level(logging.WARNING),
            patch.object(OutlookConnectionConfig, "get_client", return_value=client),
        ):
            downloader._download_message(self._file_data(), download_path)

        assert download_path.read_bytes() == SINGLE_PART_PLAIN
        assert any(record.levelno == logging.WARNING for record in caplog.records)

    @pytest.mark.parametrize("content", [{}, 123, ["a"], b"bytes"], ids=str)
    def test_an_answer_that_is_not_text_does_not_fail_the_record(
        self, tmp_path: Path, caplog, content
    ):
        """Graph breaking its own schema must not cost a complete download.

        The full message is already on disk when the answer arrives, so the
        worst case has to stay "the setting was off", not a failed record.
        """
        raw = HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes()
        downloader = self._downloader(exclude_quoted_history=True)
        client, message = self._client_writing(raw, UNIQUE_HTML)
        message.get_property.return_value = {"contentType": "html", "content": content}
        download_path = tmp_path / "msg-1.eml"

        with (
            caplog.at_level(logging.WARNING),
            patch.object(OutlookConnectionConfig, "get_client", return_value=client),
        ):
            downloader._download_message(self._file_data(), download_path)

        assert download_path.read_bytes() == raw
        assert any(record.levelno == logging.WARNING for record in caplog.records)

    def test_a_message_with_no_body_part_costs_no_request(self, tmp_path: Path):
        """Nothing can be replaced in attachment-only mail, so asking Graph for
        its unique body would spend a request on an answer with no use."""
        downloader = self._downloader(exclude_quoted_history=True)
        client, message = self._client_writing(ATTACHMENT_ONLY, UNIQUE_HTML)
        download_path = tmp_path / "msg-1.eml"

        with patch.object(OutlookConnectionConfig, "get_client", return_value=client):
            downloader._download_message(self._file_data(), download_path)

        assert download_path.read_bytes() == ATTACHMENT_ONLY
        message.select.assert_not_called()

    @pytest.mark.parametrize(
        "content",
        [
            "<html><head><style>p {color:red}</style></head><body></body></html>",
            "<html><script>alert(1)</script><body></body></html>",
            "",
        ],
        ids=["stylesheet", "script", "empty"],
    )
    def test_an_invisible_answer_preserves_the_download(self, tmp_path: Path, caplog, content):
        raw = HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes()
        downloader = self._downloader(exclude_quoted_history=True)
        client, message = self._client_writing(raw, content)
        path = tmp_path / "msg-1.eml"
        with (
            caplog.at_level(logging.WARNING),
            patch.object(OutlookConnectionConfig, "get_client", return_value=client),
        ):
            downloader._download_message(self._file_data(), path)
        assert path.read_bytes() == raw
        message.select.assert_called_once()
        assert any(record.levelno == logging.WARNING for record in caplog.records)
        assert list(tmp_path.iterdir()) == [path]

    def test_parse_failure_preserves_the_download_without_a_graph_lookup(self, tmp_path: Path):
        raw = HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes()
        downloader = self._downloader(exclude_quoted_history=True)
        client, message = self._client_writing(raw, UNIQUE_HTML)
        path = tmp_path / "msg-1.eml"
        with (
            patch.object(OutlookConnectionConfig, "get_client", return_value=client),
            patch(
                "unstructured_ingest.processes.connectors.outlook.prepare_body_replacement",
                side_effect=RecursionError("nested MIME"),
            ),
        ):
            downloader._download_message(self._file_data(), path)
        assert path.read_bytes() == raw
        message.select.assert_not_called()
        client.execute_query.assert_not_called()
        assert list(tmp_path.iterdir()) == [path]

    @pytest.mark.parametrize("protected_type", ["signed", "encrypted"])
    def test_protected_attachment_preserves_the_outer_download(
        self, tmp_path: Path, protected_type
    ):
        raw = (
            b'Content-Type: multipart/mixed; boundary="outer"\r\n\r\n'
            b"--outer\r\nContent-Type: text/plain\r\n\r\nOuter history\r\n"
            b"--outer\r\nContent-Type: message/rfc822\r\n"
            b"Content-Disposition: attachment\r\n\r\n"
            + SIGNED_MESSAGE.replace(b"multipart/signed", f"multipart/{protected_type}".encode())
            + b"\r\n--outer--\r\n"
        )
        downloader = self._downloader(exclude_quoted_history=True)
        client, message = self._client_writing(raw, "new")
        path = tmp_path / "msg-1.eml"
        with patch.object(OutlookConnectionConfig, "get_client", return_value=client):
            downloader._download_message(self._file_data(), path)
        assert path.read_bytes() == raw
        message.select.assert_not_called()

    def test_serialization_failure_preserves_the_download(self, tmp_path: Path):
        raw = HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes()
        downloader = self._downloader(exclude_quoted_history=True)
        client, message = self._client_writing(raw, UNIQUE_HTML)
        path = tmp_path / "msg-1.eml"
        with (
            patch.object(OutlookConnectionConfig, "get_client", return_value=client),
            patch(
                "email.message.EmailMessage.as_bytes", side_effect=ValueError("cannot serialize")
            ),
        ):
            downloader._download_message(self._file_data(), path)
        assert path.read_bytes() == raw
        message.select.assert_called_once()
        assert list(tmp_path.iterdir()) == [path]

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

    @pytest.mark.parametrize("failing_call", ["write_bytes", "replace"], ids=["write", "replace"])
    def test_an_unwritable_file_keeps_the_download_instead_of_failing(
        self, tmp_path: Path, failing_call: str
    ):
        """The download on disk is already complete, so a disk failure must not
        fail the record: one unwritable file would otherwise fail the whole run,
        because a non-empty pipeline status raises at the end of it."""
        raw = HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes()
        downloader = self._downloader(exclude_quoted_history=True)
        client, _ = self._client_writing(raw, UNIQUE_HTML)
        download_path = tmp_path / "msg-1.eml"

        def boom(self, *args, **kwargs):
            raise OSError("no space left on device")

        with (
            patch.object(OutlookConnectionConfig, "get_client", return_value=client),
            patch.object(Path, failing_call, boom),
        ):
            downloader._download_message(self._file_data(), download_path)

        assert download_path.read_bytes() == raw
        assert [path.name for path in tmp_path.iterdir()] == ["msg-1.eml"]

    def test_cleanup_failure_after_a_staging_failure_does_not_fail_the_record(self, tmp_path: Path):
        raw = HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes()
        downloader = self._downloader(exclude_quoted_history=True)
        client, _ = self._client_writing(raw, UNIQUE_HTML)
        download_path = tmp_path / "msg-1.eml"

        def fail_write(self, *args, **kwargs):
            raise OSError("no space left on device")

        def fail_cleanup(self, *args, **kwargs):
            raise PermissionError("cannot remove staging file")

        with (
            patch.object(OutlookConnectionConfig, "get_client", return_value=client),
            patch.object(Path, "write_bytes", fail_write),
            patch.object(Path, "unlink", fail_cleanup),
        ):
            downloader._download_message(self._file_data(), download_path)

        assert download_path.read_bytes() == raw
