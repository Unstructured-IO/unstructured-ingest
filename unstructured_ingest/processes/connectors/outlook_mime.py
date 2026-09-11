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
from copy import deepcopy
from dataclasses import dataclass
from email.message import EmailMessage
from enum import Enum
from typing import Optional, Union

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

# Rewriting a body would break the signature these carry. One anywhere in the
# tree is enough to decline the whole message, an attached signed message
# included: a signature covers the exact bytes it was made over, and rebuilding
# the message re-serialises every part, not only the one being replaced.
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
    """The two supported MIME body renderings."""

    HTML = "html"
    TEXT = "plain"


class KeepOriginal(Enum):
    """Why a message should remain as downloaded."""

    NO_BODY_PART = "has no body part to reduce"
    PROTECTED = "is signed or encrypted"
    EMPTY_ORIGINAL = "has an empty body"
    MISSING_CONTENT = "has no replacement body text"
    RENDERING_MISMATCH = "has a replacement in a different or unknown rendering"


class _MailMessage(EmailMessage):
    """A message whose named inline parts count as attachments.

    The standard library calls a part an attachment only when its disposition
    says so, which lets a named inline text part win body selection over the
    real body. Parsing through this class makes `get_body` and the sweep below
    agree on what an attachment is.
    """

    def is_attachment(self) -> bool:
        return super().is_attachment() or self.get_filename() is not None


def _parse(raw: bytes) -> _MailMessage:
    """Parse a downloaded message, with every part a `_MailMessage`."""
    return email.message_from_bytes(raw, _MailMessage, policy=MIME_POLICY)


def _has_visible_text(content: str, rendering: BodyRendering) -> bool:
    """Whether a body value holds any words a reader would see.

    A body with no words still arrives as a non-empty string of tags, which
    would otherwise replace a full body with nothing. Stripping tags is wrong
    for plain text though, where "<no comment>" is the whole message, so the
    rendering decides which reading applies.
    """
    if not content:
        return False
    if rendering is not BodyRendering.HTML:
        return bool(content.strip())
    rendered = _MARKUP_TAG.sub(" ", _UNRENDERED_ELEMENT.sub(" ", content))
    return bool(html.unescape(rendered).strip())


@dataclass(frozen=True)
class ReplacementBody:
    """Replacement text and the rendering its provider returned."""

    rendering: Optional[BodyRendering]
    content: str


class PreparedReplacement:
    """An eligible body prepared by `prepare_body_replacement`, with private MIME state."""

    def __init__(
        self, message: _MailMessage, part: EmailMessage, rendering: BodyRendering, has_text: bool
    ) -> None:
        self._message = message
        self._part = part
        self._rendering = rendering
        self._has_text = has_text

    @property
    def rendering(self) -> BodyRendering:
        """The rendering required by the selected body part."""
        return self._rendering

    def replace(self, body: Optional[ReplacementBody]) -> Union[bytes, KeepOriginal]:
        """Serialize an accepted replacement without changing this prepared message."""
        if body is not None and body.rendering is not self.rendering:
            return KeepOriginal.RENDERING_MISMATCH
        if body is None or not _has_visible_text(body.content, self.rendering):
            return KeepOriginal.MISSING_CONTENT if self._has_text else KeepOriginal.EMPTY_ORIGINAL

        # Copy together so the selected part remains a member of the copied tree.
        message, part = deepcopy((self._message, self._part))
        preserved = [
            (name, value) for name, value in part.items() if name.lower() in _KEPT_BODY_HEADERS
        ]
        had_mime_version = "MIME-Version" in part
        # Base64 cannot contain a MIME delimiter line, even if the text contains one.
        # set_content also removes obsolete format=flowed and delsp parameters.
        part.set_content(body.content, subtype=self.rendering.value, charset="utf-8", cte="base64")
        if not had_mime_version:
            del part["MIME-Version"]
        for name, value in preserved:
            part[name] = value
        _drop_other_body_renderings(message, part)
        return message.as_bytes(policy=MIME_POLICY)


def prepare_body_replacement(raw: bytes) -> Union[PreparedReplacement, KeepOriginal]:
    """Select an eligible body and the rendering its replacement must use."""
    message = _parse(raw)
    if any(part.get_content_type() in _PROTECTED_CONTENT_TYPES for part in message.walk()):
        return KeepOriginal.PROTECTED

    part = message.get_body(preferencelist=BODY_PART_PREFERENCE)
    if part is None:
        return KeepOriginal.NO_BODY_PART

    rendering = BodyRendering(part.get_content_subtype())
    try:
        text = part.get_content()
    except Exception:  # noqa: BLE001 - an undecodable body holds no readable text
        text = ""
    return PreparedReplacement(
        message=message,
        part=part,
        rendering=rendering,
        has_text=_has_visible_text(text, rendering),
    )


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
