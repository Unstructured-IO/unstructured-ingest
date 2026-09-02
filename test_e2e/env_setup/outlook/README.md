# Outlook fixture seeding

`seed_fixtures.py` guarantees that a fixed, known set of fixture messages exists in the
Outlook mailbox the `outlook.sh` end-to-end test reads from, before that test runs. It is
create-if-absent: every time it runs, it checks whether each fixture already exists and only
creates the ones that do not.

Fixture *content* lives in `fixtures.py`, one `SeedMessage` per fixture, each with a comment
explaining the specific question that fixture exists to answer. This file explains how the two
scripts fit into CI and why they have to work the way they do.

This is a public repository. Every fixture is generic, synthetic test mail: no customer name,
no real correspondence, no deployment-specific detail, anywhere in `fixtures.py`, in this file,
or in commit history touching either. If you add a fixture, keep it that way.

## How this gets invoked from CI

`test_e2e/src/outlook.sh` calls `seed_fixtures.py` itself, right after its existing check that
`MS_CLIENT_ID` / `MS_CLIENT_CRED` / `MS_TENANT_ID` / `MS_USER_EMAIL` are all set (and before it
runs the actual ingest). `outlook.sh` is itself invoked by `test_e2e/test-src.sh`'s main loop,
which is invoked by the `src_e2e_test` job in `.github/workflows/e2e.yml` (nightly / on-demand /
post-merge only, never on pull requests) and by the `update-fixtures-and-pr` job in
`.github/workflows/ingest-test-fixtures-update-pr.yml`. Both jobs already set the four
credentials above from GitHub secrets and already install this project's `outlook` extra
(`msal` + `Office365-REST-Python-Client`), so no CI configuration beyond this directory and the
two-line `test-src.sh` gate fix (below) is needed to make seeding run.

Because seeding sits inside `outlook.sh` rather than in its own `all_tests` entry, a seeding
failure fails `outlook.sh` itself (via `set -e`, already at the top of that script) with the
message this script printed to stderr, before any ingest is attempted. `test-src.sh` then
treats that the same as any other test failure: a non-8, non-zero exit code stops the run
(`test-src.sh` lines ~86-101 as of this change).

Run it by hand the same way `outlook.sh` does, from the repository root:

```bash
PYTHONPATH=. test_e2e/env_setup/outlook/seed_fixtures.py
```

It reads `MS_CLIENT_ID`, `MS_CLIENT_CRED`, `MS_TENANT_ID`, `MS_USER_EMAIL` from the environment
and nothing else. It never prints, logs, or writes any of their values; only fixture slugs,
folder names, and Graph error codes appear in its output.

## Why seeding has to be create-if-absent

The mailbox this seeds into is a real, shared, persistent Microsoft 365 mailbox, not a
container this test suite owns and can tear down between runs the way `couchbase.sh` and
`sftp.sh` do. Recreating every fixture from scratch on every run would work functionally, but
it would silently defeat the fixture-comparison mechanism the rest of `test_e2e` relies on --
see the next section for exactly why.

## The `sha256(message.id)` consequence

`OutlookIndexer._generate_fullpath` (`unstructured_ingest/processes/connectors/outlook.py`)
names every downloaded fixture file `sha256(message.id)[:16] + ".eml"`. `message.id` is an
opaque identifier Microsoft Graph assigns when a message is created; this script never
chooses it and cannot predict it ahead of creation.

`test_e2e/check-diff-expected-output.py` (`check_files`) requires the *set* of output filenames
to exactly match the set of files already committed under
`test_e2e/expected-structured-output/outlook/`. Put those two facts together: if this script
tore fixtures down and recreated them every run, every fixture would get a brand-new
Graph-assigned `id` every run, therefore a brand-new sha256-derived filename every run, and
`check_files` would fail on every single run, permanently, by design -- not because anything is
actually wrong, but because the committed expected-output filenames can never keep up with
freshly-minted ids. Create-if-absent is what keeps `message.id`, and therefore the filename,
stable across runs: the same physical Graph messages persist, so the same hashes keep matching
the same committed `<hash>.eml.json` files.

This is also why `MOVE_TARGET` (see `fixtures.py`) is the one fixture worth reading closely if
you're new to this: the vendored Graph client's own `Message.move()` docstring says a move
creates a new copy and deletes the original, which means a *moved* message gets a new id too.
Idempotency here cannot key off `message.id` at all -- it keys off a separate, stable marker
this script controls (below), specifically so that survives.

## Idempotency marker, not `message.id`

Each fixture is tagged, at creation time, with a `singleValueLegacyExtendedProperty` whose
`value` is the fixture's `slug` (see `fixtures.py`) and whose `id` is a fixed,
script-owned GUID (`seed_fixtures.MARKER_PROPERTY_ID`). Before creating anything, the script
filters the seed folder for a message carrying that GUID with the target slug as its value; if
one is found, that fixture is skipped.

The vendored client's own convenience method for this, `Message.add_extended_property()`,
mints a fresh random GUID on every call, which would make every fixture "invisible" to every
later run's existence check. This script builds the property dict directly instead, at one
fixed GUID, for exactly that reason.

## Permissions

Every Graph call here needs only the **Mail.ReadWrite** application permission -- creating
messages and drafts, tagging them, moving them, flagging them, marking them read, categorizing
them. Nothing in this script calls send, reply, or forward (the operations that actually
transmit mail and need **Mail.Send**); replies and forwards are created as Graph drafts
(`createReply` / `createForward`) and left unsent.

If the Azure AD app registration behind `MS_CLIENT_ID` does not hold Mail.ReadWrite, or holds
it but has not been granted tenant admin consent, the first write call will come back `403`.
This script treats that specifically as a **provisioning problem**, not a script bug: it prints
a distinct fatal message naming Mail.ReadWrite and the app registration, rather than a generic
traceback, and exits `2`. A missing seed folder is also its own distinct message and exit code
(`3`) rather than folding into the same path. Missing environment variables exit `1`, and name
only which variables are missing, never any value.

## Open questions

- Whether `createReply` / `createForward` place the new draft in Drafts by default, or next to
  the original, was not confirmed against a live mailbox (this session was not permitted to
  touch one). The script does not depend on the answer -- it explicitly moves the resulting
  draft into the seed folder either way -- but a same-folder-to-same-folder move is an
  unexercised path worth a first-run sanity check.
- The first time this script actually runs against the real mailbox, it will create fixtures
  whose `message.id` (and therefore filename) cannot be known ahead of time. `check-diff-expected-output.py`
  already has a documented escape hatch for exactly this ("export OVERWRITE_FIXTURES=true and
  rerun"); whoever runs seeding for the first time needs to use it once, review the new
  `expected-structured-output/outlook/*.json` files it produces, and commit them. This PR does
  not and cannot do that step itself.
- Whether the mailbox's three pre-existing committed fixtures predate this marker (almost
  certainly yes) and whether they should stay, was not resolved here; this script does not
  touch or remove anything it did not itself create.
