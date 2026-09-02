"""Fixture mail definitions seeded into the Outlook integration-test mailbox.

This module holds data only: what each fixture message is, and the one question it exists
to answer. seed_fixtures.py holds the logic that turns this data into Graph API calls.

Every fixture is generic, synthetic test mail. None of it names a customer, a deployment, or
any real correspondence -- see the module docstring in seed_fixtures.py and the README in this
directory for why that matters (this repository is public).

Each SeedMessage.slug is a permanent idempotency key (see seed_fixtures.py): once a slug has
shipped, its wording may still change, but the slug string itself must not, or the script will
create a second, duplicate message under the old identity instead of recognizing the existing one.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Folder the connector is configured to read (test_e2e/src/outlook.sh: --outlook-folders).
SEED_FOLDER_NAME = "EmailsWithAttachments"

# Graph well-known folder name used as the "somewhere else" starting point for MOVE_TARGET.
# Using the well-known literal "drafts" means there's no separate staging folder to create
# and idempotency-check on its own; every mailbox already has exactly one Drafts folder.
STAGING_WELL_KNOWN_FOLDER = "drafts"

POST_CREATE_MARK_READ = "mark_read"
POST_CREATE_FLAG = "flag"
POST_CREATE_CATEGORIZE = "categorize"


@dataclass(frozen=True)
class Attachment:
    """A small file attachment added to a message at creation time."""

    filename: str
    content_type: str
    text_content: str


@dataclass(frozen=True)
class SeedMessage:
    """One fixture message the script guarantees exists in SEED_FOLDER_NAME.

    Only one of reply_to / forward_of / attachment / stage_in_well_known_folder is expected
    to be set per fixture; seed_fixtures.py dispatches creation on whichever is set. A plain
    message (none of them set) is created directly in the seed folder via folder.messages.add.
    """

    slug: str
    subject: str
    body_text: str
    comment: str
    body_content_type: str = "Text"  # "Text" or "HTML"
    reply_to: str | None = None  # slug of the message this is a Graph createReply of
    forward_of: str | None = None  # slug of the message this is a Graph createForward of
    attachment: Attachment | None = None
    stage_in_well_known_folder: str | None = None
    post_create_actions: tuple[str, ...] = field(default_factory=tuple)


THREAD_STARTER = SeedMessage(
    slug="thread-starter",
    subject="[unstructured-ingest fixture] weekly status placeholder",
    body_text=(
        "This is a placeholder message used only by unstructured-ingest's automated tests. "
        "It intentionally carries no real content."
    ),
    comment=(
        "Answers nothing on its own. It exists only so THREAD_REPLY and THREAD_FORWARD have "
        "a real prior message to attach Graph-native threading to."
    ),
)

THREAD_REPLY = SeedMessage(
    slug="thread-reply",
    subject=THREAD_STARTER.subject,
    body_text="Thanks, no action needed on my end.",
    reply_to=THREAD_STARTER.slug,
    comment=(
        "A reply sitting inside an existing thread, not a standalone message. Answers: does "
        "the connector surface Graph's own conversationId / reply relationship correctly, "
        "instead of every message looking like an unrelated one-off?"
    ),
)

THREAD_FORWARD = SeedMessage(
    slug="thread-forward",
    subject=THREAD_STARTER.subject,
    body_text="Forwarding for visibility, nothing needed from you.",
    forward_of=THREAD_STARTER.slug,
    comment=(
        "A forward of the same starter message, sent back to the same mailbox so no second "
        "address is needed. Answers: does the connector distinguish a forward from a reply "
        "and from a standalone message, rather than collapsing all three to the same shape?"
    ),
)

INLINE_QUOTED_HISTORY = SeedMessage(
    slug="inline-quoted-history",
    subject="[unstructured-ingest fixture] re: placeholder thread",
    body_content_type="HTML",
    body_text=(
        "<p>Following up on the note below.</p>"
        "<blockquote>"
        "<p>On Mon, Jan 1, 2001, Fixture Sender &lt;fixture-sender@example.com&gt; wrote:</p>"
        "<p>&gt; This is the quoted portion of an earlier message, embedded directly in the "
        "body text&lt;br&gt;&gt; rather than represented through Graph's reply/forward "
        "relationship.</p>"
        "</blockquote>"
    ),
    comment=(
        "A message whose body contains manually embedded quoted history (the way a mail "
        "client renders an inline quote), as opposed to Graph-native threading. Answers: "
        "does the partitioner handle an inline blockquote in HTML body without mistaking "
        "the quoted portion for the message's own primary content?"
    ),
)

SMALL_ATTACHMENT = SeedMessage(
    slug="small-attachment",
    subject="[unstructured-ingest fixture] note with attachment",
    body_text="See attached.",
    attachment=Attachment(
        filename="fixture-note.txt",
        content_type="text/plain",
        text_content=(
            "This attachment is deliberately tiny and boring.\n"
            "It exists only so the partitioner has a real attachment to open.\n"
        ),
    ),
    comment=(
        "A message carrying one small, deliberately dull attachment. Answers: does the "
        "connector's downloader and the hosted partitioning API round-trip a message that "
        "actually has attachment bytes -- has_attachments true, attachment content "
        "partitioned -- without the fixture drifting because of what the attachment says. "
        "An elaborate attachment would give the hosted partitioner more surface area to "
        "change its output on, silently breaking this fixture on unrelated model updates."
    ),
)

MOVE_TARGET = SeedMessage(
    slug="move-target",
    subject="[unstructured-ingest fixture] moved into place",
    body_text="This message did not start in the seed folder; the seeding script moved it here.",
    stage_in_well_known_folder=STAGING_WELL_KNOWN_FOLDER,
    comment=(
        "A message created in Drafts, then explicitly moved into the seed folder. Graph's "
        "move creates a new copy and deletes the original (see office365 Message.move's own "
        "docstring), which assigns a brand-new id and therefore a brand-new "
        "sha256(message.id)-derived fixture filename. This fixture specifically exercises "
        "that the idempotency marker -- not message.id -- is what lets a second run of this "
        "script recognize the message as already seeded."
    ),
)

FLAG_READ_CATEGORIZE_TARGET = SeedMessage(
    slug="flag-read-categorize-target",
    subject="[unstructured-ingest fixture] mutable state placeholder",
    body_text="This message exists to be flagged, marked read, and categorized after creation.",
    post_create_actions=(POST_CREATE_MARK_READ, POST_CREATE_FLAG, POST_CREATE_CATEGORIZE),
    comment=(
        "A message the script marks read, flags for follow-up, and assigns a category to, "
        "all after creation. Answers: does the connector's metadata mapping correctly "
        "reflect a message's mutable state (is_read, flag, categories) rather than only the "
        "content it was created with?"
    ),
)

# Order matters: seed_fixtures.py processes these tuples left to right and keeps a slug -> handle
# map of what it has created or found so far. A fixture that sets reply_to/forward_of looks its
# base up in that map, so the base must appear earlier in the seeding order than any fixture
# that depends on it (checked below, since getting this wrong would fail as a runtime KeyError
# deep inside a Graph call sequence instead of at import time).
ALL_FIXTURES: tuple[SeedMessage, ...] = (
    THREAD_STARTER,
    INLINE_QUOTED_HISTORY,
    SMALL_ATTACHMENT,
    MOVE_TARGET,
    FLAG_READ_CATEGORIZE_TARGET,
)

# Not seeded by default: neither thread fixture currently seeds when its base message was only
# ever created via the raw message-create API and never actually sent, and THREAD_STARTER is
# exactly such a message. Against the real mailbox, createReply 400s (ErrorInvalidReferenceItem)
# and createForward fails client-side inside the SDK with no HTTP response at all. The root cause
# is unconfirmed (Microsoft documents no sent-through-mail-flow constraint), so seeding these two
# is opt-in (SEED_DEFERRED_FIXTURES=1 in the environment) until a Graph-native threading route is
# settled; a default run then succeeds on the five fixtures that do work instead of always
# exiting non-zero. When opted in they are seeded after ALL_FIXTURES, so their THREAD_STARTER
# base is already in the handle map.
DEFERRED_FIXTURES: tuple[SeedMessage, ...] = (
    THREAD_REPLY,
    THREAD_FORWARD,
)

_slugs = [f.slug for f in ALL_FIXTURES + DEFERRED_FIXTURES]
assert len(_slugs) == len(set(_slugs)), "fixture slugs must be unique: %r" % _slugs
for _i, _f in enumerate(ALL_FIXTURES + DEFERRED_FIXTURES):
    for _dep in (_f.reply_to, _f.forward_of):
        assert _dep is None or _dep in _slugs[:_i], (
            f"fixture '{_f.slug}' depends on '{_dep}', which must appear earlier in "
            "the seeding order than the fixture that depends on it"
        )
del _slugs, _i, _f, _dep
