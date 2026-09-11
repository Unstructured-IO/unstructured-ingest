"""Message shapes the Outlook body-replacement tests are built from.

Not a test module: it holds the fixtures and the "nothing else changed"
assertions shared by test_outlook_mime.py and test_outlook_quoted_history.py.
"""

from pathlib import Path

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


def _leaves(message):
    return [part for part in message.walk() if not part.is_multipart()]


def _is_unnamed_text_part(part):
    """Fixture classification independent of the production MIME implementation."""
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
        if not _is_unnamed_text_part(part)
    ]


def _header_facts(message) -> list[tuple[str, str]]:
    """Headers a body replacement has no business changing."""
    structural = {"content-type", "content-transfer-encoding", "mime-version"}
    return sorted(
        (name.lower(), str(value))
        for name, value in message.items()
        if name.lower() not in structural
    )
