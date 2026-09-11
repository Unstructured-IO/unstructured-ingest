import email
import email.policy
import re
from email.message import EmailMessage
from pathlib import Path
from typing import Optional

import pytest

from test.unit.connectors.outlook_messages import (
    ATTACHMENT_ONLY,
    EMPTY_BODY,
    HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT,
    INLINE_IMAGE_RELATED,
    LONG_NON_ASCII_HEADERS,
    NESTED_SENTINEL,
    SIGNED_MESSAGE,
    SINGLE_PART_PLAIN,
    STRUCTURED_FIXTURES,
    UNIQUE_HTML,
    _header_facts,
    _leaves,
    _non_body_facts,
)
from unstructured_ingest.processes.connectors.outlook_mime import (
    BODY_PART_PREFERENCE,
    BodyRendering,
    BodyReplacement,
    KeepFullBody,
    _is_unnamed_text_part,
    apply_body_replacement,
    has_visible_text,
    parse,
    plan_body_replacement,
)


def _reduced(raw: bytes, unique_content: str) -> bytes:
    """The bytes the downloader would write for this message and this unique body.

    Plan, then apply: the two calls the downloader makes once Graph has
    answered, in the order it makes them. Nothing is decided here that the
    connector does not decide for itself.
    """
    plan = plan_body_replacement(raw)
    assert isinstance(plan, BodyReplacement), f"expected a plan, got {plan}"
    return apply_body_replacement(plan, unique_content)


def _declines(raw: bytes, unique_content: Optional[str]) -> bool:
    """Whether the downloader would keep the message exactly as downloaded.

    The shipped conditions, in the shipped order: no part that may be rewritten,
    no usable answer from Graph (which is what UniqueBody.from_graph reports as
    None), or an answer with no words in it.
    """
    plan = plan_body_replacement(raw)
    if isinstance(plan, KeepFullBody):
        return True
    if unique_content is None:
        return True
    return not has_visible_text(unique_content, plan.rendering)


class TestPlanBodyReplacement:
    """The part reported must be the one a partitioner will read.

    Its subtype decides which rendering Graph is asked for, so a wrong answer
    here puts the wrong markup into the message.
    """

    @pytest.mark.parametrize("fixture", STRUCTURED_FIXTURES, ids=lambda p: p.name)
    def test_prefers_html_when_the_message_carries_both(self, fixture: Path):
        part = plan_body_replacement(fixture.read_bytes()).part
        assert part is not None
        assert part.get_content_subtype() == "html"

    def test_reports_plain_when_there_is_no_html_rendering(self):
        part = plan_body_replacement(SINGLE_PART_PLAIN).part
        assert part is not None
        assert part.get_content_subtype() == "plain"

    def test_reports_none_when_there_is_no_body_part(self):
        assert plan_body_replacement(ATTACHMENT_ONLY) is KeepFullBody.NO_BODY_PART

    def test_excludes_a_named_inline_html_part(self):
        raw = (
            b'Content-Type: multipart/mixed; boundary="mix"\r\n\r\n'
            b"--mix\r\nContent-Type: text/plain\r\n\r\nThe actual message body.\r\n"
            b"--mix\r\nContent-Type: text/html\r\n"
            b'Content-Disposition: inline; filename="report.html"\r\n\r\n'
            b"<p>Attached report contents.</p>\r\n--mix--\r\n"
        )
        stdlib = email.message_from_bytes(raw, policy=email.policy.default)
        assert stdlib.get_body(preferencelist=BODY_PART_PREFERENCE).get_filename() == (
            "report.html"
        ), "premise: the standard library selects the named inline part as the body"

        body = plan_body_replacement(raw).part

        assert body is not None
        assert body.get_content_type() == "text/plain"
        assert "actual message body" in body.get_content()


class TestBodyReplacement:
    """The surgery must change the body and nothing else.

    Losing an attachment here would be worse than the duplication this
    feature exists to remove, so every invariant gets its own assertion.
    """

    @pytest.mark.parametrize("fixture", STRUCTURED_FIXTURES, ids=lambda p: p.name)
    def test_non_body_parts_are_untouched(self, fixture: Path):
        raw = fixture.read_bytes()
        before = _non_body_facts(parse(raw))

        reduced = _reduced(raw, UNIQUE_HTML)

        assert reduced is not None
        assert _non_body_facts(parse(reduced)) == before

    def test_a_single_part_message_keeps_its_identifying_headers(self):
        """The body part is the message itself here, so the surgery rewrites the
        top-level content type and transfer encoding by design. Everything that
        identifies the message still has to survive."""
        reduced = _reduced(SINGLE_PART_PLAIN, "Only the newest sentence.")

        assert reduced is not None
        rebuilt, original = parse(reduced), parse(SINGLE_PART_PLAIN)
        for header in ("Subject", "From", "To"):
            assert str(rebuilt[header]) == str(original[header])
        assert "Only the newest sentence." in rebuilt.get_content()
        assert "The original body text." not in rebuilt.get_content()

    @pytest.mark.parametrize("fixture", STRUCTURED_FIXTURES, ids=lambda p: p.name)
    def test_headers_are_untouched(self, fixture: Path):
        raw = fixture.read_bytes()
        before = _header_facts(parse(raw))

        reduced = _reduced(raw, UNIQUE_HTML)

        assert _header_facts(parse(reduced)) == before

    def test_long_non_ascii_headers_are_not_refolded(self):
        """The default serialization policy rewrites long source headers.

        Pinned separately from the parametrized case because a fixture with
        only short headers cannot catch it: the failure is whitespace inserted
        inside a header the replacement never touched.
        """
        raw = LONG_NON_ASCII_HEADERS.read_bytes()
        original = parse(raw)
        long_headers = [name for name, value in original.items() if len(f"{name}: {value}") > 200]
        assert long_headers, "fixture no longer carries a long header to protect"

        reduced = _reduced(raw, UNIQUE_HTML)

        rebuilt = parse(reduced)
        for name in long_headers:
            assert str(rebuilt[name]) == str(original[name])

    @pytest.mark.parametrize("fixture", STRUCTURED_FIXTURES, ids=lambda p: p.name)
    def test_the_body_part_carries_the_supplied_text(self, fixture: Path):
        reduced = _reduced(fixture.read_bytes(), UNIQUE_HTML)

        body = parse(reduced).get_body(preferencelist=BODY_PART_PREFERENCE)
        assert "Only the newest sentence." in body.get_content()

    @pytest.mark.parametrize("fixture", STRUCTURED_FIXTURES, ids=lambda p: p.name)
    def test_no_other_body_rendering_survives(self, fixture: Path):
        """A stale plain-text rendering would still hold the quoted history.

        The partitioner accepts a setting that flips its preference to plain
        text, which would silently restore that history and make the whole
        feature a no-op, so the other renderings are removed rather than left.
        """
        raw = fixture.read_bytes()
        assert len([p for p in _leaves(parse(raw)) if _is_unnamed_text_part(p)]) > 1

        reduced = _reduced(raw, UNIQUE_HTML)

        assert len([p for p in _leaves(parse(reduced)) if _is_unnamed_text_part(p)]) == 1

    @pytest.mark.parametrize("fixture", STRUCTURED_FIXTURES, ids=lambda p: p.name)
    def test_the_original_body_text_is_gone(self, fixture: Path):
        raw = fixture.read_bytes()
        original_body = parse(raw).get_body(preferencelist=BODY_PART_PREFERENCE)
        # Markup is stripped rather than used to skip lines: skipping any line
        # containing a tag leaves nothing to assert on an HTML body.
        words = [
            word
            for word in re.sub(r"<[^>]+>", " ", original_body.get_content()).split()
            if len(word) > 8 and word.isalpha()
        ]
        assert words, "fixture body has no distinctive words to check"

        reduced = _reduced(raw, UNIQUE_HTML)

        rebuilt_text = parse(reduced).get_body(preferencelist=BODY_PART_PREFERENCE).get_content()
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

        reduced = _reduced(raw, "Only the newest sentence.")

        assert reduced is not None
        rebuilt = parse(reduced)
        named = next(part for part in rebuilt.walk() if part.get_filename() == "report.html")
        assert named.get_content_disposition() == "inline"
        assert "Attached report contents." in named.get_content()
        body = plan_body_replacement(reduced).part
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
        original_body = parse(raw).get_body(preferencelist=BODY_PART_PREFERENCE)
        assert original_body is not None
        assert original_body["Content-ID"] == "<root>"

        reduced = _reduced(raw, UNIQUE_HTML)

        assert reduced is not None
        rebuilt = parse(reduced)
        body = rebuilt.get_body(preferencelist=BODY_PART_PREFERENCE)
        assert body is not None
        assert body["Content-ID"] == "<root>"
        assert "Only the newest sentence." in body.get_content()


class TestBodyReplacementLeavesNestedMessagesAlone:
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
            part.get_content_type() == "message/rfc822" for part in parse(outer.as_bytes()).walk()
        )
        return outer.as_bytes()

    def test_the_attached_message_body_survives(self):
        raw = self._message_with_an_attached_message()

        reduced = _reduced(raw, UNIQUE_HTML)

        assert reduced is not None
        assert NESTED_SENTINEL in reduced.decode("utf-8", "replace")

    def test_only_the_outer_bodys_own_rendering_is_removed(self):
        raw = self._message_with_an_attached_message()

        reduced = _reduced(raw, UNIQUE_HTML)

        before = len(_leaves(parse(raw)))
        after = len(_leaves(parse(reduced)))
        assert after == before - 1

    def test_the_outer_body_is_still_replaced(self):
        raw = self._message_with_an_attached_message()

        reduced = _reduced(raw, UNIQUE_HTML)

        body = parse(reduced).get_body(preferencelist=BODY_PART_PREFERENCE)
        assert "Only the newest sentence." in body.get_content()
        assert "outer plain body" not in reduced.decode("utf-8", "replace")


class TestBodyReplacementSweepsNestedContainers:
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
        assert parse(raw).get_body(preferencelist=BODY_PART_PREFERENCE).get_content_type() == (
            "text/html"
        )
        return raw

    def test_the_plain_rendering_in_another_container_is_removed(self):
        reduced = _reduced(self._nested(), UNIQUE_HTML)

        assert reduced is not None
        assert b"PLAINRENDERING" not in reduced

    def test_the_inline_image_survives(self):
        reduced = _reduced(self._nested(), UNIQUE_HTML)

        types = [part.get_content_type() for part in _leaves(parse(reduced))]
        assert "image/png" in types

    def test_no_empty_container_is_left_behind(self):
        """The alternative group holds nothing once its only rendering goes."""
        reduced = _reduced(self._nested(), UNIQUE_HTML)

        containers = [
            part for part in parse(reduced).walk() if part.is_multipart() and not part.get_payload()
        ]
        assert containers == []


class TestBodyReplacementKeepsInlineImages:
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

        reduced = _reduced(raw, self._body_with_a_reference())

        assert reduced is not None
        types = [part.get_content_type() for part in _leaves(parse(reduced))]
        assert "image/png" in types

    def test_the_reference_itself_survives(self):
        raw = INLINE_IMAGE_RELATED.read_bytes()

        reduced = _reduced(raw, self._body_with_a_reference())

        body = parse(reduced).get_body(preferencelist=BODY_PART_PREFERENCE)
        assert "cid:img1" in body.get_content()


class TestBodyReplacementLeavesAttachedContainersAlone:
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

        reduced = _reduced(raw, UNIQUE_HTML)

        assert reduced is not None
        assert b"ATTACHEDBUNDLETEXT" in reduced

    def test_the_outer_plain_rendering_is_still_removed(self):
        raw = self._message_with_an_attached_container()

        reduced = _reduced(raw, UNIQUE_HTML)

        assert b"Outer plain rendering." not in reduced
        body = parse(reduced).get_body(preferencelist=BODY_PART_PREFERENCE)
        assert "Only the newest sentence." in body.get_content()


class TestBodyReplacementLineEndings:
    def test_the_rebuilt_message_uses_crlf(self):
        """Graph delivers CRLF, and the default policy would flatten it."""
        reduced = _reduced(HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes(), UNIQUE_HTML)

        assert reduced is not None
        assert b"\r\n" in reduced
        assert reduced.replace(b"\r\n", b"").count(b"\n") == 0


class TestBodyReplacementDeclines:
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

        assert _declines(raw, unique_content)

    def test_declines_when_there_is_no_body_part_to_replace(self):
        assert _declines(ATTACHMENT_ONLY, UNIQUE_HTML)

    def test_declines_for_an_entity_only_value(self):
        """Non-breaking spaces are not text, and would blank a real body.

        The fixture has an HTML body, so the value is read as markup. Against a
        plain-text body the same string is literal text and is written, which
        TestCarriesText covers.
        """
        raw = HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes()

        assert _declines(raw, "<p>&nbsp;&nbsp;</p>")

    def test_an_empty_body_and_an_empty_value_leave_the_message_alone(self):
        assert _declines(EMPTY_BODY, "")


class TestHasVisibleText:
    """Angle brackets are markup in an HTML body and characters in a text one."""

    @pytest.mark.parametrize(
        "value",
        [None, "", "   ", "<div></div>", "<p>&nbsp;</p>", "<p>&#160;&#160;</p>"],
        ids=["none", "empty", "spaces", "tags", "nbsp-entity", "numeric-entity"],
    )
    def test_markup_without_words_is_not_text(self, value):
        assert has_visible_text(value, BodyRendering.HTML) is False

    @pytest.mark.parametrize(
        "value",
        ["hello", "<p>hello</p>", "<p>&amp;</p>"],
        ids=["bare", "wrapped", "escaped-ampersand"],
    )
    def test_markup_with_words_is_text(self, value):
        assert has_visible_text(value, BodyRendering.HTML) is True

    @pytest.mark.parametrize(
        "value",
        ["<no comment>", "<see attached>", "a < b and c > d"],
        ids=["bracketed-note", "bracketed-pointer", "comparison"],
    )
    def test_plain_text_in_angle_brackets_is_still_text(self, value):
        assert has_visible_text(value, BodyRendering.TEXT) is True

    @pytest.mark.parametrize(
        "value",
        ["<no comment>", "<see attached>"],
        ids=["bracketed-note", "bracketed-pointer"],
    )
    def test_the_same_values_read_as_empty_markup(self, value):
        """This is the miss the flag exists to prevent. Read as markup these
        strip to nothing, so a plain-text message would be left unreduced."""
        assert has_visible_text(value, BodyRendering.HTML) is False

    @pytest.mark.parametrize("value", [None, "", "  \r\n "], ids=["none", "empty", "whitespace"])
    def test_plain_text_still_has_to_hold_something(self, value):
        assert has_visible_text(value, BodyRendering.TEXT) is False

    @pytest.mark.parametrize(
        "value",
        [
            "<html><head><style>p.MsoNormal {margin:0cm; font-size:11.0pt}</style></head>"
            "<body><p class=MsoNormal></p></body></html>",
            "<html><head><script>var trackingId = 'abc123';</script></head><body></body></html>",
            "<STYLE TYPE='text/css'>@media print { body { color: black } }</STYLE>",
        ],
        ids=["outlook-stylesheet", "script", "shouting-stylesheet"],
    )
    def test_a_stylesheet_or_a_script_is_not_words(self, value):
        """Outlook writes a stylesheet into a body that says nothing at all.

        Stripping only the tags would leave the rule set behind and read it as
        words, so a body that says something would be replaced by one that says
        nothing rather than being kept as it is.
        """
        assert has_visible_text(value, BodyRendering.HTML) is False

    def test_words_beside_a_stylesheet_are_still_words(self):
        value = "<html><head><style>p {margin:0}</style></head><body><p>new text</p></body></html>"

        assert has_visible_text(value, BodyRendering.HTML) is True


class TestBodyReplacementKeepsBodyPartHeaders:
    """set_content clears every Content-* header on the part it rewrites.

    Those headers say what the part is and where it sits, not what it holds:
    a multipart/related start= points at Content-ID, and Outlook mail resolves
    relative references to inline resources through Content-Location.
    """

    RELATED_BODY = (
        b"From: sender@example.com\r\nSubject: related\r\nMIME-Version: 1.0\r\n"
        b'Content-Type: multipart/related; boundary="rel"; start="<body@x>"\r\n\r\n'
        b"--rel\r\nContent-Type: text/html; charset=utf-8\r\n"
        b"Content-ID: <body@x>\r\n"
        b"Content-Location: http://example.invalid/mail.htm\r\n"
        b"Content-Base: http://example.invalid/\r\n"
        b"Content-Language: en-GB\r\n"
        b"Content-Description: the message body\r\n"
        b"Content-Disposition: inline\r\n\r\n"
        b"<html><body>quoted history</body></html>\r\n\r\n"
        b"--rel\r\nContent-Type: image/png\r\nContent-ID: <img@x>\r\n"
        b"Content-Transfer-Encoding: base64\r\n\r\naGk=\r\n\r\n--rel--\r\n"
    )

    @pytest.mark.parametrize(
        "header",
        [
            "Content-ID",
            "Content-Location",
            # Non-standard, and the fallback Outlook resolves relative references
            # against when Content-Location is absent, so losing it silently
            # breaks inline resources rather than raising.
            "Content-Base",
            "Content-Language",
            "Content-Description",
            "Content-Disposition",
        ],
    )
    def test_the_header_survives_the_replacement(self, header: str):
        reduced = _reduced(self.RELATED_BODY, UNIQUE_HTML)

        body = parse(reduced).get_body(preferencelist=BODY_PART_PREFERENCE)
        assert (
            body[header]
            == parse(self.RELATED_BODY).get_body(preferencelist=BODY_PART_PREFERENCE)[header]
        )

    def test_the_container_start_parameter_still_resolves(self):
        reduced = parse(_reduced(self.RELATED_BODY, UNIQUE_HTML))

        start = reduced.get_param("start")
        assert start is not None
        assert any(part["Content-ID"] == start for part in reduced.iter_parts())

    def test_no_mime_version_is_added_to_a_sub_part(self):
        """RFC 2045 defines MIME-Version for the outermost entity only."""
        reduced = _reduced(self.RELATED_BODY, UNIQUE_HTML)

        assert reduced.count(b"MIME-Version") == 1


class TestBodyReplacementWithTwoTextBlocks:
    def test_a_second_narrative_text_part_is_dropped(self):
        """Some clients write text, an image, then more text, side by side.

        The second block is swept as though it were another rendering. Graph
        returns the unique body as one piece covering both blocks and it is
        written into the first part, so the text is moved rather than lost.
        Pinned because nothing else states it, and because the sweep is where
        a future change would silently start losing the text for real.
        """
        raw = (
            b"From: sender@example.com\r\nSubject: two blocks\r\nMIME-Version: 1.0\r\n"
            b'Content-Type: multipart/mixed; boundary="mix"\r\n\r\n'
            b"--mix\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nFIRST BLOCK\r\n\r\n"
            b"--mix\r\nContent-Type: image/png\r\nContent-Transfer-Encoding: base64\r\n\r\n"
            b"aGk=\r\n\r\n"
            b"--mix\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nSECOND BLOCK\r\n\r\n"
            b"--mix--\r\n"
        )

        reduced = _reduced(raw, "the whole unique body")

        assert b"SECOND BLOCK" not in reduced
        assert b"the whole unique body" in reduced
        assert b"image/png" in reduced


class TestBodyReplacementLeavesProtectedMessagesAlone:
    def test_a_signed_message_declines(self):
        assert _declines(SIGNED_MESSAGE, UNIQUE_HTML)

    def test_an_encrypted_message_declines(self):
        encrypted = SIGNED_MESSAGE.replace(b"multipart/signed", b"multipart/encrypted")

        assert _declines(encrypted, UNIQUE_HTML)


class TestBodyReplacementDoesNotForgeAPart:
    """A body is written out verbatim, so its text can look like a delimiter.

    The generator reuses a boundary the message already carries without
    scanning the payload for it, and neither 7bit nor quoted-printable escapes
    a leading "--". A line matching a container's delimiter would therefore be
    re-read as a real one: the message silently gains a part it never had, or
    ends early with an attachment still in the file but unreachable.
    """

    @pytest.mark.parametrize("fixture", STRUCTURED_FIXTURES, ids=lambda p: p.name)
    @pytest.mark.parametrize("suffix", ["", "--"], ids=["delimiter", "terminator"])
    def test_a_body_carrying_a_container_delimiter_changes_nothing_else(
        self, fixture: Path, suffix: str
    ):
        raw = fixture.read_bytes()
        original = parse(raw)
        boundaries = [
            part.get_boundary() for part in original.walk() if part.get_boundary() is not None
        ]
        assert boundaries, "premise: the fixture nests its parts inside a container"
        # A delimiter is only read as one when it starts its own line, so each
        # forged marker has to, or the test proves nothing.
        forged = "".join(f"\r\n--{boundary}{suffix}" for boundary in boundaries)

        reduced = parse(
            _reduced(raw, f"<html><body><p>opening</p>{forged}\r\n<p>closing</p></body></html>")
        )

        body = reduced.get_body(preferencelist=BODY_PART_PREFERENCE)
        assert "opening" in body.get_content()
        assert "closing" in body.get_content(), "the body ended at a forged delimiter"
        assert _non_body_facts(reduced) == _non_body_facts(original)
        assert [defect for part in reduced.walk() for defect in part.defects] == []

    def test_the_body_still_holds_the_whole_value(self):
        raw = HTML_AND_PLAIN_WITH_IMAGE_ATTACHMENT.read_bytes()
        boundary = parse(raw).get_boundary()
        value = f"<html><body><p>before</p>\r\n--{boundary}--\r\n<p>after</p></body></html>"

        body = parse(_reduced(raw, value)).get_body(preferencelist=BODY_PART_PREFERENCE)

        assert "before" in body.get_content()
        assert "after" in body.get_content()


class TestBodyReplacementKeepsRelatedSiblings:
    """In a related bundle only the root part renders the body.

    get_body picks the start= part and never looks at its siblings, so a text
    sibling is a resource the root references, not a second rendering. Sweeping
    one would leave a cid: reference pointing at nothing.
    """

    RELATED_WITH_TEXT_SIBLING = (
        b"From: sender@example.com\r\nSubject: related\r\nMIME-Version: 1.0\r\n"
        b'Content-Type: multipart/related; boundary="rel"; start="<root@x>"\r\n\r\n'
        b"--rel\r\nContent-Type: text/html; charset=utf-8\r\nContent-ID: <root@x>\r\n\r\n"
        b'<html><body>quoted history<iframe src="cid:frag@x"></iframe></body></html>\r\n\r\n'
        b"--rel\r\nContent-Type: text/html; charset=utf-8\r\nContent-ID: <frag@x>\r\n"
        b"Content-Disposition: inline\r\n\r\n<p>A REFERENCED FRAGMENT</p>\r\n\r\n--rel--\r\n"
    )

    def test_a_referenced_text_sibling_survives(self):
        reduced = _reduced(self.RELATED_WITH_TEXT_SIBLING, UNIQUE_HTML)

        assert b"A REFERENCED FRAGMENT" in reduced

    def test_every_content_id_the_body_references_still_resolves(self):
        reduced = parse(
            _reduced(
                self.RELATED_WITH_TEXT_SIBLING,
                '<html><body><p>new</p><iframe src="cid:frag@x"></iframe></body></html>',
            )
        )

        present = {part["Content-ID"] for part in reduced.walk() if part["Content-ID"]}
        body = reduced.get_body(preferencelist=BODY_PART_PREFERENCE)
        referenced = {f"<{cid}>" for cid in re.findall(r'cid:([^"\'\s>]+)', body.get_content())}
        assert referenced <= present

    def test_the_root_is_still_the_part_that_was_rewritten(self):
        reduced = parse(_reduced(self.RELATED_WITH_TEXT_SIBLING, UNIQUE_HTML))

        body = reduced.get_body(preferencelist=BODY_PART_PREFERENCE)
        assert body["Content-ID"] == "<root@x>"
        assert "Only the newest sentence." in body.get_content()


class TestBodyReplacementWithDeeplyNestedContainers:
    """The sweep recurses, so an emptied container has to cascade upwards."""

    def _three_levels(self) -> bytes:
        outer = EmailMessage()
        outer["Subject"] = "three levels"
        outer["From"] = "sender@example.com"
        outer.set_content("the plain rendering")
        outer.add_alternative("<html><body>the html rendering</body></html>", subtype="html")
        outer.add_attachment(
            b"\x89PNG\r\n", maintype="image", subtype="png", filename="picture.png"
        )
        outer.add_attachment(b"%PDF-1.4", maintype="application", subtype="pdf", filename="a.pdf")
        return outer.as_bytes()

    def test_no_empty_container_survives_the_sweep(self):
        reduced = parse(_reduced(self._three_levels(), UNIQUE_HTML))

        empty = [part for part in reduced.walk() if part.is_multipart() and not part.get_payload()]
        assert empty == []

    def test_both_attachments_survive(self):
        raw = self._three_levels()

        reduced = parse(_reduced(raw, UNIQUE_HTML))

        assert _non_body_facts(reduced) == _non_body_facts(parse(raw))

    def test_only_one_rendering_is_left(self):
        reduced = parse(_reduced(self._three_levels(), UNIQUE_HTML))

        renderings = [part for part in _leaves(reduced) if _is_unnamed_text_part(part)]
        assert len(renderings) == 1
        assert "Only the newest sentence." in renderings[0].get_content()
