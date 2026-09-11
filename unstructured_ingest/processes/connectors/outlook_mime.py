"""Replacing a mail message's body in place, without disturbing anything else in it.

Nothing here knows about Graph. The caller decides what the new body text is; this
module decides which part holds it, whether that part may be rewritten at all, and
how to put the text back so that every attachment, inline resource and header
arrives exactly as it was downloaded.
"""

import email
import email.policy
import html
import re
from dataclasses import dataclass
from email.message import EmailMessage
from enum import Enum
from typing import Any, Optional, Union
from uuid import uuid4

# The only two renderings the email partitioner will read (its
# VALID_CONTENT_SOURCES), in its own order of preference. It can be configured
# to prefer plain, so these are also the only two that can hide quoted history.
BODY_PART_PREFERENCE = ("html", "plain")

# refold_source="none" stops the default policy rewriting long headers this
# rebuild never touches: it split an Authentication-Results header on a real
# message. linesep keeps Graph's CRLF rather than flattening to bare newlines.
MIME_POLICY = email.policy.default.clone(refold_source="none", linesep="\r\n")

_MARKUP_TAG = re.compile(r"<[^>]+>")

# Markup whose own text is never shown to a reader. An Outlook body carries a
# stylesheet even when it says nothing, so counting the text inside these would
# read a rule set as words and replace a full body with a sheet of CSS.
_UNRENDERED_ELEMENT = re.compile(r"<(script|style)\b[^>]*>.*?</\1\s*>", re.IGNORECASE | re.DOTALL)

# Rewriting a body would break the signature these carry.
_PROTECTED_CONTENT_TYPES = ("multipart/signed", "multipart/encrypted")

# Headers that say what a body part is and where it sits, rather than what it
# holds. A multipart/related start= points at Content-ID, and Outlook mail
# resolves relative references to inline resources through Content-Location
# (RFC 2557).
_KEPT_BODY_HEADERS = frozenset(
    {
        "content-id",
        "content-location",
        "content-base",
        "content-language",
        "content-description",
        "content-disposition",
    }
)


class BodyRendering(Enum):
    """A rendering of a message body, under both of the names it goes by.

    MIME calls plain text "plain"; Graph's Prefer header and its answer call the
    same rendering "text". Carrying both names on one member is what keeps the
    two vocabularies from being translated by hand at each boundary, and what
    lets "the rendering we asked for" and "the rendering we got" be compared
    rather than string-matched.
    """

    HTML = ("html", "html")
    TEXT = ("plain", "text")

    def __init__(self, mime_subtype: str, graph_value: str) -> None:
        self.mime_subtype = mime_subtype
        self.graph_value = graph_value

    @property
    def is_markup(self) -> bool:
        """Whether angle brackets in this rendering are tags rather than text."""
        return self is BodyRendering.HTML

    @classmethod
    def of_part(cls, part: EmailMessage) -> "BodyRendering":
        """The rendering a body part holds. Body selection yields only these two."""
        return cls.HTML if part.get_content_subtype() == cls.HTML.mime_subtype else cls.TEXT

    @classmethod
    def of_graph(cls, content_type: Any) -> Optional["BodyRendering"]:
        """The rendering Graph says it answered in, or None if it did not say.

        The value is a plain string on office365 2.x and a BodyType enum on 3.x.
        Anything else means Graph named a rendering this cannot honour, which is
        not the same as naming the one that was asked for.
        """
        value = getattr(content_type, "value", content_type)
        if not isinstance(value, str):
            return None
        named = value.lower()
        return next((rendering for rendering in cls if rendering.graph_value == named), None)


class KeepFullBody(Enum):
    """Why a message's body cannot be replaced, in words that finish a log line."""

    NO_BODY_PART = "has no body part to reduce"
    PROTECTED = "is signed or encrypted"


class MailMessage(EmailMessage):
    """A message whose named inline parts count as attachments.

    The standard library calls a part an attachment only when its disposition
    says so, which lets a named inline text part win body selection over the
    real body. Parsing through this class makes `get_body` and the sweep below
    agree on what an attachment is.
    """

    def is_attachment(self) -> bool:
        return super().is_attachment() or self.get_filename() is not None


def parse(raw: bytes) -> MailMessage:
    """Parse a downloaded message, with every part a `MailMessage`."""
    return email.message_from_bytes(raw, MailMessage, policy=MIME_POLICY)


def has_visible_text(content: str, rendering: BodyRendering) -> bool:
    """Whether a body value holds any words a reader would see.

    A body with no words still arrives as a non-empty string of tags, which
    would otherwise replace a full body with nothing. Stripping tags is wrong
    for plain text though, where "<no comment>" is the whole message, so the
    rendering decides which reading applies.
    """
    if not content:
        return False
    if not rendering.is_markup:
        return bool(content.strip())
    rendered = _MARKUP_TAG.sub(" ", _UNRENDERED_ELEMENT.sub(" ", content))
    return bool(html.unescape(rendered).strip())


@dataclass(frozen=True)
class BodyReplacement:
    """The body part a reduction would rewrite, and the rendering it must be given.

    `part_has_text` is read before the rewrite, because it is the only way to
    tell a message whose body was always empty from one whose body was lost.
    """

    message: MailMessage
    part: EmailMessage
    rendering: BodyRendering
    part_has_text: bool


def plan_body_replacement(raw: bytes) -> Union[BodyReplacement, KeepFullBody]:
    """What replacing the body of `raw` would rewrite, or why it must not be tried.

    This is the only place that decides which part a partitioner will read and
    which rendering has to be asked for, so the caller never has to derive
    either for itself.
    """
    message = parse(raw)
    if any(part.get_content_type() in _PROTECTED_CONTENT_TYPES for part in message.walk()):
        return KeepFullBody.PROTECTED

    part = message.get_body(preferencelist=BODY_PART_PREFERENCE)
    if part is None:
        return KeepFullBody.NO_BODY_PART

    rendering = BodyRendering.of_part(part)
    try:
        text = part.get_content()
    except Exception:  # noqa: BLE001 - an undecodable body holds no readable text
        text = ""
    return BodyReplacement(
        message=message,
        part=part,
        rendering=rendering,
        part_has_text=has_visible_text(text, rendering),
    )


def apply_body_replacement(plan: BodyReplacement, content: str) -> bytes:
    """The planned message with its body part holding only `content`.

    set_content clears every Content-* header on the part and adds a
    MIME-Version a sub-part should not carry, so the headers describing this
    part's identity and place in the message are put back afterwards.
    """
    part = plan.part
    preserved = [
        (name, value) for name, value in part.items() if name.lower() in _KEPT_BODY_HEADERS
    ]
    had_mime_version = "MIME-Version" in part

    part.set_content(content, subtype=plan.rendering.mime_subtype, charset="utf-8")

    if not had_mime_version:
        del part["MIME-Version"]
    for name, value in preserved:
        part[name] = value

    _drop_other_body_renderings(plan.message, part)
    _refresh_forged_boundaries(plan.message, content)
    return plan.message.as_bytes(policy=MIME_POLICY)


def _related_root(container: EmailMessage) -> Optional[EmailMessage]:
    """The one child of a multipart/related that renders the body, if this is one.

    Mirrors what get_body does with a related bundle: only the root part is a
    rendering of the body, and every sibling is a resource it references. A
    sibling swept as though it were a second rendering would leave a cid:
    reference pointing at nothing.
    """
    if container.get_content_subtype() != "related":
        return None
    children = container.get_payload()
    if not isinstance(children, list) or not children:
        return None

    start = container.get_param("start")
    if start:
        for child in children:
            if child.get("Content-ID") == start:
                return child
    return children[0]


def _drop_other_body_renderings(part: EmailMessage, keep: EmailMessage) -> None:
    """Remove every rendering of this message's body except `keep`.

    Descent stops at anything attached, an attached message included: its own
    body parts are indistinguishable from this message's and must survive.
    A container emptied by the sweep is dropped rather than left unreadable.
    """
    if not part.is_multipart():
        return
    children = part.get_payload()
    if not isinstance(children, list):
        return

    related_root = _related_root(part)
    kept = []
    for child in children:
        if related_root is not None and child is not related_root:
            # A resource the bundle's root references, not a rendering of the body.
            kept.append(child)
            continue
        if child.get_content_maintype() == "message" or child.is_attachment():
            kept.append(child)
        elif child.is_multipart():
            _drop_other_body_renderings(child, keep)
            grandchildren = child.get_payload()
            if not isinstance(grandchildren, list) or grandchildren:
                kept.append(child)
        elif child is keep or not _is_unnamed_text_part(child):
            kept.append(child)

    if len(kept) != len(children):
        part.set_payload(kept)


def _is_unnamed_text_part(part: EmailMessage) -> bool:
    """Whether a part is a rendering of the message body rather than a file."""
    return part.get_content_type() in ("text/plain", "text/html") and not part.is_attachment()


def _refresh_forged_boundaries(message: EmailMessage, content: str) -> None:
    """Re-boundary any container whose delimiter the new body text could forge.

    A text part is written out verbatim under 7bit and quoted-printable alike,
    and the generator reuses a boundary the message already carries without
    scanning the payload for it. A line in the new body matching a container's
    delimiter would therefore be re-read as a real one on the next parse: the
    message silently gains a part it never had, or ends early with any
    attachment past that point still in the file but unreachable.
    """
    for part in message.walk():
        boundary = part.get_boundary()
        if boundary and any(line.startswith(f"--{boundary}") for line in content.splitlines()):
            part.set_boundary(uuid4().hex)
