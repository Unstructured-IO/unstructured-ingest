#!/usr/bin/env python3
"""Create-if-absent seeding for the Outlook connector's CI fixture mailbox.

Run from the repository root (matching test_e2e/src/outlook.sh's own convention) with:

    PYTHONPATH=. test_e2e/env_setup/outlook/seed_fixtures.py

See the README in this directory for why this has to be create-if-absent rather than
tear-down-and-recreate, and for how CI actually invokes it.

Credentials (MS_CLIENT_ID, MS_CLIENT_CRED, MS_TENANT_ID, MS_USER_EMAIL) are read from the
environment only. They are never printed, logged, or written anywhere by this script. Only
non-secret material -- fixture slugs, folder names, Graph error codes -- appears in output.

Every Graph call this script makes needs only the Mail.ReadWrite application permission. It
never calls send/reply/forward (the API operations that transmit real mail), only the
create-a-draft equivalents; see README.md for why.
"""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fixtures import (  # noqa: E402
    ALL_FIXTURES,
    DEFERRED_FIXTURES,
    POST_CREATE_CATEGORIZE,
    POST_CREATE_FLAG,
    POST_CREATE_MARK_READ,
    SEED_FOLDER_NAME,
    SeedMessage,
)

if TYPE_CHECKING:
    from office365.graph_client import GraphClient
    from office365.outlook.mail.folders.folder import MailFolder
    from office365.outlook.mail.messages.message import Message

REQUIRED_ENV_VARS = ("MS_CLIENT_ID", "MS_CLIENT_CRED", "MS_TENANT_ID", "MS_USER_EMAIL")

# Fixed, script-owned marker used as the idempotency key for every fixture this script creates.
# The GUID has no meaning beyond namespacing this property away from anything else that might
# set extended properties in the same mailbox; it must never change once fixtures exist under it,
# or every existing fixture would look "absent" on the next run and get recreated as a duplicate.
# Format matches Microsoft Graph's singleValueLegacyExtendedProperty id convention, confirmed
# directly against the vendored client's own (unrelated) use of it in
# office365/outlook/mail/messages/message.py Message.add_extended_property.
_MARKER_GUID = "d529fed5-efc1-4260-86dd-4cbc201d634d"
_MARKER_NAME = "UnsIngestFixtureId"
MARKER_PROPERTY_ID = f"String {{{_MARKER_GUID}}} Name {_MARKER_NAME}"


def _require_env_vars() -> dict[str, str]:
    """Read required credentials from the environment, or fail loudly without exposing them.

    Only variable *names* are ever printed here, never values.
    """
    missing = [name for name in REQUIRED_ENV_VARS if not os.environ.get(name)]
    if missing:
        print(
            "FATAL: missing required environment variable(s): " + ", ".join(missing),
            file=sys.stderr,
        )
        print(
            "This script reads credentials from the environment only; set these and re-run.",
            file=sys.stderr,
        )
        sys.exit(1)
    return {name: os.environ[name] for name in REQUIRED_ENV_VARS}


def _build_client(env: dict[str, str]) -> "GraphClient":
    """Build an authenticated Graph client via the connector's own config/auth classes.

    Reusing OutlookAccessConfig/OutlookConnectionConfig from the connector itself -- rather
    than re-implementing MSAL token acquisition here -- means this script exercises the exact
    same auth path the connector uses, and this script's own code never sees a raw token or
    client secret; both stay inside the imported classes.
    """
    # Imported lazily so a missing/broken outlook extra fails inside _require_env_vars's
    # caller with a normal traceback rather than at module import time.
    from unstructured_ingest.processes.connectors.outlook import (
        OutlookAccessConfig,
        OutlookConnectionConfig,
    )

    connection_config = OutlookConnectionConfig(
        access_config=OutlookAccessConfig(client_cred=env["MS_CLIENT_CRED"]),
        client_id=env["MS_CLIENT_ID"],
        tenant=env["MS_TENANT_ID"],
    )
    return connection_config.get_client()


def _run(client: "GraphClient", description: str) -> None:
    """Execute whatever Graph queries are currently queued on `client`.

    A 403 here means the Azure AD app registration behind MS_CLIENT_ID does not hold (or has
    not been granted tenant admin consent for) the permission this call needed. That is a
    provisioning problem for whoever owns the app registration, not a bug in this script, so
    it gets a distinct, loud message and its own exit code instead of a bare stack trace.
    """
    from office365.runtime.client_request_exception import ClientRequestException

    try:
        client.execute_query()
    except ClientRequestException as exc:
        status = getattr(exc.response, "status_code", None)
        code = exc.code
        if status == 403:
            print(
                f"FATAL while {description}: Microsoft Graph returned 403 Forbidden.",
                file=sys.stderr,
            )
            print(
                "The Azure AD app registration behind MS_CLIENT_ID does not currently hold "
                "(or has not been granted tenant admin consent for) the Mail.ReadWrite "
                "application permission this call needed. This is a provisioning problem "
                "with the app registration, not a bug in this script -- fix the "
                "registration's permissions/consent, then re-run.",
                file=sys.stderr,
            )
            if code:
                print(f"Graph error code: {code}", file=sys.stderr)
            sys.exit(2)
        print(f"FATAL while {description}: Graph request failed (HTTP {status}).", file=sys.stderr)
        if code:
            print(f"Graph error code: {code}", file=sys.stderr)
        raise


def _find_seed_folder(client: "GraphClient", user_email: str) -> "MailFolder":
    """Find SEED_FOLDER_NAME among the mailbox's root folders, case-insensitively.

    Mirrors OutlookIndexer._get_selected_root_folders' own matching rule (outlook.py) so a
    folder this script considers "found" is the same one the connector will read from. Uses
    an explicit .top() beyond Graph's default page size, which that method does not, so this
    lookup does not miss the folder in a mailbox with many root folders.
    """
    user = client.users[user_email]
    root_folders = user.mail_folders
    root_folders.get().top(999)
    _run(client, "listing root mail folders")
    for folder in root_folders:
        if (folder.display_name or "").lower() == SEED_FOLDER_NAME.lower():
            return folder
    print(
        f"FATAL: no root mail folder named '{SEED_FOLDER_NAME}' exists for {user_email}.",
        file=sys.stderr,
    )
    print(
        "This script only seeds fixtures into an existing folder; it does not create the "
        "seed folder itself. Create it once (e.g. via Outlook) and re-run.",
        file=sys.stderr,
    )
    sys.exit(3)


def _fixture_exists(client: "GraphClient", folder: "MailFolder", slug: str) -> "Message | None":
    """Return the existing fixture message for `slug`, or None if it has not been seeded yet.

    Scoped to `folder` only (not recursive): every fixture this script creates ends up living
    directly in the seed folder by the time creation finishes (MOVE_TARGET is moved there
    explicitly; replies/forwards are moved there after creation -- see _create_reply_fixture /
    _create_forward_fixture), so a non-recursive check of the seed folder is sufficient.
    """
    filter_expr = (
        "singleValueExtendedProperties/Any(ep: ep/id eq '{}' and ep/value eq '{}')"
    ).format(MARKER_PROPERTY_ID, slug)
    matches = folder.messages
    matches.get().filter(filter_expr).top(1)
    _run(client, f"checking whether fixture '{slug}' already exists")
    return matches[0] if len(matches) > 0 else None


def _marker_property_value(slug: str) -> list[dict]:
    """The singleValueExtendedProperties value (a one-item list) that tags a message as `slug`.

    Deliberately not using the vendored client's own Message.add_extended_property(): it
    mints a fresh random uuid4 as the property's GUID on every call (see
    office365/outlook/mail/messages/message.py), which makes it useless as a *stable*,
    later-queryable key. Building the property dict directly, keyed on one fixed, script-owned
    GUID (MARKER_PROPERTY_ID), makes the same marker id findable on every future run.
    """
    return [{"id": MARKER_PROPERTY_ID, "value": slug}]


def _marker_kwarg(slug: str) -> dict:
    """The singleValueExtendedProperties kwarg to merge into a folder.messages.add(...) call."""
    return {"singleValueExtendedProperties": _marker_property_value(slug)}


def _create_reply_capturing_draft(original: "Message", comment: str) -> "Message":
    """Create a reply draft off `original`, returning a handle to the NEW draft.

    office365's own Message.create_reply() (message.py) builds a `return_type` object meant
    to receive the createReply response, then returns `self` -- the ORIGINAL message --
    instead of that return_type. Contrast with MailFolder.copy() in the same client, which
    returns its own return_type correctly; create_reply's `return self` looks like an
    oversight rather than an intentional fluent-interface choice. Calling .move() on
    create_reply()'s return value would silently move the ORIGINAL seed message, not the new
    reply. This reproduces the same call exactly, with only that return value fixed.
    """
    from office365.outlook.mail.messages.message import Message
    from office365.runtime.queries.service_operation import ServiceOperationQuery

    new_draft = Message(original.context)
    qry = ServiceOperationQuery(
        original, "createReply", None, {"comment": comment}, None, new_draft
    )
    original.context.add_query(qry)
    return new_draft


def _create_forward_capturing_draft(
    original: "Message", to_recipients: list[str], comment: str
) -> "Message":
    """Create a forward draft off `original`, returning a handle to the NEW draft.

    Same return-value bug as create_reply (see _create_reply_capturing_draft) in office365's
    Message.create_forward(). Also note create_forward's own ToRecipients handling does not
    convert plain email strings the way MessageCollection.add's to_recipients does -- it
    expects Recipient objects already built, so Recipient.from_email is applied here.
    """
    from office365.outlook.mail.messages.message import Message
    from office365.outlook.mail.recipient import Recipient
    from office365.runtime.client_value_collection import ClientValueCollection
    from office365.runtime.queries.service_operation import ServiceOperationQuery

    new_draft = Message(original.context)
    payload = {
        "ToRecipients": ClientValueCollection(
            Recipient, [Recipient.from_email(addr) for addr in to_recipients]
        ),
        "Message": None,
        "Comment": comment,
    }
    qry = ServiceOperationQuery(original, "createForward", None, payload, None, new_draft)
    original.context.add_query(qry)
    return new_draft


def _create_plain_fixture(
    client: "GraphClient", folder: "MailFolder", fixture: SeedMessage
) -> "Message":
    from office365.outlook.mail.item_body import ItemBody

    body = ItemBody(content=fixture.body_text, content_type=fixture.body_content_type)
    msg = folder.messages.add(subject=fixture.subject, body=body, **_marker_kwarg(fixture.slug))
    if fixture.attachment is not None:
        msg.add_file_attachment(
            name=fixture.attachment.filename,
            content=fixture.attachment.text_content,
            content_type=fixture.attachment.content_type,
        )
    _run(client, f"creating fixture '{fixture.slug}'")
    return msg


def _create_reply_fixture(
    client: "GraphClient", folder: "MailFolder", fixture: SeedMessage, base: "Message"
) -> "Message":
    draft = _create_reply_capturing_draft(base, fixture.body_text)
    _run(client, f"creating reply draft for fixture '{fixture.slug}'")

    # Tag the marker in a follow-up call: createReply's request body only accepts `comment`,
    # so the idempotency marker cannot be set in the same call that creates the draft.
    draft.set_property("singleValueExtendedProperties", _marker_property_value(fixture.slug))
    draft.update()
    _run(client, f"tagging fixture '{fixture.slug}' with its idempotency marker")

    # Always move the draft into the seed folder explicitly rather than trusting where Graph
    # happened to place it: whether createReply's draft defaults into Drafts or next to the
    # original was not confirmed against a live mailbox (see README "Open questions"), so this
    # makes the end state correct either way instead of depending on that undocumented default.
    draft.move(folder)
    _run(client, f"moving fixture '{fixture.slug}' into {SEED_FOLDER_NAME}")
    return draft


def _create_forward_fixture(
    client: "GraphClient",
    folder: "MailFolder",
    fixture: SeedMessage,
    base: "Message",
    user_email: str,
) -> "Message":
    # Forwarded to the same mailbox that owns it, so no second address needs to exist.
    draft = _create_forward_capturing_draft(base, [user_email], fixture.body_text)
    _run(client, f"creating forward draft for fixture '{fixture.slug}'")

    draft.set_property("singleValueExtendedProperties", _marker_property_value(fixture.slug))
    draft.update()
    _run(client, f"tagging fixture '{fixture.slug}' with its idempotency marker")

    draft.move(folder)
    _run(client, f"moving fixture '{fixture.slug}' into {SEED_FOLDER_NAME}")
    return draft


def _create_staged_fixture(
    client: "GraphClient", folder: "MailFolder", fixture: SeedMessage, user_email: str
) -> "Message":
    from office365.outlook.mail.item_body import ItemBody

    staging_folder = client.users[user_email].mail_folders[fixture.stage_in_well_known_folder]
    body = ItemBody(content=fixture.body_text, content_type=fixture.body_content_type)
    msg = staging_folder.messages.add(
        subject=fixture.subject, body=body, **_marker_kwarg(fixture.slug)
    )
    _run(client, f"creating fixture '{fixture.slug}' in {fixture.stage_in_well_known_folder}")

    msg.move(folder)
    _run(client, f"moving fixture '{fixture.slug}' into {SEED_FOLDER_NAME}")
    return msg


def _apply_post_create_actions(client: "GraphClient", fixture: SeedMessage, msg: "Message") -> None:
    if not fixture.post_create_actions:
        return
    if POST_CREATE_MARK_READ in fixture.post_create_actions:
        msg.set_property("isRead", True)
    if POST_CREATE_FLAG in fixture.post_create_actions:
        from office365.outlook.mail.messages.followup_flag import FollowupFlag

        # FollowupFlag's own constructor defaults completed/due/start_datetime to a fresh
        # DateTimeTimeZone() rather than None (office365/outlook/mail/messages/followup_flag.py).
        # A raw {"flagStatus": ...} dict passed to set_property() merges into that broken
        # default instead of replacing it (ClientObject.set_property's dict branch), so every
        # unset date field still serializes as a present-but-empty {} sub-object, and Graph
        # rejects the implied empty timeZone with TimeZoneNotSupportedException. Passing a
        # fully-constructed FollowupFlag with the three date fields explicitly nulled bypasses
        # the broken default and sends only flagStatus, which is all this fixture wants.
        msg.set_property(
            "flag",
            FollowupFlag(
                flag_status="flagged",
                completed_datetime=None,
                due_datetime=None,
                start_datetime=None,
            ),
        )
    if POST_CREATE_CATEGORIZE in fixture.post_create_actions:
        msg.set_property("categories", ["unstructured-ingest-fixture"])
    msg.update()
    _run(client, f"applying post-create actions to fixture '{fixture.slug}'")


def _seed_one(
    client: "GraphClient",
    folder: "MailFolder",
    fixture: SeedMessage,
    handles: dict[str, "Message"],
    user_email: str,
) -> None:
    existing = _fixture_exists(client, folder, fixture.slug)
    if existing is not None:
        print(f"exists, skipping: {fixture.slug}")
        handles[fixture.slug] = existing
        return

    print(f"creating: {fixture.slug}")
    if fixture.reply_to is not None:
        msg = _create_reply_fixture(client, folder, fixture, handles[fixture.reply_to])
    elif fixture.forward_of is not None:
        base = handles[fixture.forward_of]
        msg = _create_forward_fixture(client, folder, fixture, base, user_email)
    elif fixture.stage_in_well_known_folder is not None:
        msg = _create_staged_fixture(client, folder, fixture, user_email)
    else:
        msg = _create_plain_fixture(client, folder, fixture)

    _apply_post_create_actions(client, fixture, msg)
    handles[fixture.slug] = msg


def main() -> int:
    env = _require_env_vars()
    client = _build_client(env)
    user_email = env["MS_USER_EMAIL"]

    folder = _find_seed_folder(client, user_email)

    handles: dict[str, "Message"] = {}
    failed: list[str] = []
    fixtures_to_seed = ALL_FIXTURES + (
        DEFERRED_FIXTURES if os.environ.get("SEED_DEFERRED_FIXTURES") else ()
    )
    for fixture in fixtures_to_seed:
        # A fixture whose base failed to seed has nothing to attach to; skip it without
        # attempting the call at all, rather than letting it fail on a KeyError that would
        # misreport as a problem of its own.
        base_slug = fixture.reply_to or fixture.forward_of
        if base_slug is not None and base_slug not in handles:
            print(f"skipping (base '{base_slug}' did not seed): {fixture.slug}", file=sys.stderr)
            failed.append(fixture.slug)
            continue
        try:
            _seed_one(client, folder, fixture, handles, user_email)
        except Exception as exc:
            # A real provisioning problem (missing permission, missing seed folder) exits the
            # process directly via sys.exit() inside _run() and is deliberately NOT caught here,
            # since SystemExit does not subclass Exception. This only isolates one fixture's own
            # Graph request failing, so the run still attempts every other fixture instead of
            # aborting on the first surprise.
            print(f"skipping '{fixture.slug}' after failure: {exc}", file=sys.stderr)
            failed.append(fixture.slug)

    seeded = len(fixtures_to_seed) - len(failed)
    print(
        f"done: {seeded}/{len(fixtures_to_seed)} fixture(s) confirmed present in "
        f"{SEED_FOLDER_NAME}"
    )
    if failed:
        print(f"NOT seeded, needs follow-up: {', '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
