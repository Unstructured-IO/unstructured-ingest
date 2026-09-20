"""Live integration coverage for the Teradata SQL destination.

Covers the upload AND two rejections. The rejections are the point: a row the server refuses is
classified rather than written, and it is the classification that has been wrong, so a row-count
assertion alone would stay green through the defect this file follows.

DEPENDS ON THE WIDENED CODE MAP. The rejection tests assert `UserError`, which needs 2801 and 6706
in `_USER_FAULT_TERADATA_CODES`; until the PR that widens that map lands, both provocations fall
through to `DestinationConnectionError`, whose message is "Failed to connect to server <host>".
That is the exact misdiagnosis these tests exist to catch, so rather than fail as one they skip on
the map itself, and the skip clears itself the moment the map is widened.

RUNNING IT. Not wired into CI. `test_databricks_delta_tables.py` in this directory is the nearest
precedent but not the same mechanism: the Databricks credentials ARE plumbed into the
`sql_connectors_int_test` job, and that test is inert only because it carries an unconditional
`@pytest.mark.skip` on top of its `@requires_env`. These carry only `@requires_env`, so they would
start running the first time anyone put `TERADATA_*` into the job environment; nothing under
`.github/` sets those today. Teradata has no container image and no emulator, and the only free
endpoint is a 60-day trial (Teradata's ClearScape Analytics Experience), so a credential in CI
secrets would rot and then fail as an auth error rather than a skip.

    export TERADATA_HOST=...        # host or IP; the driver uses port 1025
    export TERADATA_USER=...
    export TERADATA_PASSWORD=...
    export TERADATA_DATABASE=...    # a database the user may CREATE TABLE in
    uv run --locked --no-sync pytest test/integration/connectors/sql/test_teradata.py -v

Never print the raw teradatasql message: the Go driver appends connection text that embeds host,
user and password. That has to hold for the scaffolding, not just the assertions, so every driver
call here goes through `get_cursor`, which re-raises a driver failure as a redacted
`TeradataTestError`. A wrong `TERADATA_PASSWORD` on a first run is the likely way an unredacted
message would otherwise reach the terminal. The rejection tests capture one real driver message on
purpose, compare the connector's user-facing message against it, and keep it out of assertion
output.
"""

import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, Optional
from uuid import uuid4

import pandas as pd
import pytest
from pydantic import BaseModel, SecretStr

from test.integration.connectors.utils.constants import (
    DESTINATION_TAG,
    SQL_TAG,
    env_setup_path,
)
from test.integration.connectors.utils.validation.destination import (
    StagerValidationConfigs,
    stager_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import UserError, safe_error_summary
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connectors.sql.teradata import (
    _USER_FAULT_TERADATA_CODES,
    CONNECTOR_TYPE,
    TeradataAccessConfig,
    TeradataConnectionConfig,
    TeradataUploader,
    TeradataUploaderConfig,
    TeradataUploadStager,
    TeradataUploadStagerConfig,
    _extract_teradata_error_code,
    _is_teradata_driver_error,
)

REQUIRED_ENV = ("TERADATA_HOST", "TERADATA_USER", "TERADATA_PASSWORD", "TERADATA_DATABASE")

# Codes the server raises for the two rejections provoked below. Neither is in
# `_USER_FAULT_TERADATA_CODES` yet, which is what `needs_widened_code_map` gates on.
DUPLICATE_KEY_CODE = 2801
UNTRANSLATABLE_CHARACTER_CODE = 6706
# "object does not exist": Teradata has no DROP TABLE IF EXISTS, so teardown swallows this one.
TABLE_DOES_NOT_EXIST_CODE = 3807

# A character with no LATIN representation, for the 6706 provocation. Kept as an escape rather
# than a literal so the file stays ASCII and the intent is legible.
NON_LATIN_TEXT = "\u4e2d\u6587"

# The columns the strict fixture accepts, in the order the raw provocations bind them.
STRICT_COLUMNS = ("id", "record_id", "element_id", "text", "type")

needs_widened_code_map = pytest.mark.skipif(
    not {DUPLICATE_KEY_CODE, UNTRANSLATABLE_CHARACTER_CODE} <= set(_USER_FAULT_TERADATA_CODES),
    reason=(
        f"needs {DUPLICATE_KEY_CODE}/{UNTRANSLATABLE_CHARACTER_CODE} in "
        "_USER_FAULT_TERADATA_CODES (unstructured-ingest#812); without them the provocation "
        "raises DestinationConnectionError and reads as a connect failure"
    ),
)


class TeradataTestError(RuntimeError):
    """A driver failure, redacted, carrying the server's code.

    An exception that escapes a fixture is printed verbatim by pytest, and the teradatasql
    message embeds host, user and password. The numeric code is kept so callers can still
    branch on it (teardown swallowing 3807, say).
    """

    def __init__(self, exc: Exception) -> None:
        self.code: Optional[int] = _extract_teradata_error_code(exc)
        super().__init__(f"teradata call failed: {safe_error_summary(exc)} (code={self.code})")


class EnvData(BaseModel):
    host: str
    user: str
    password: SecretStr
    database: str


def get_env_data() -> EnvData:
    return EnvData(
        host=os.environ["TERADATA_HOST"],
        user=os.environ["TERADATA_USER"],
        password=os.environ["TERADATA_PASSWORD"],
        database=os.environ["TERADATA_DATABASE"],
    )


def get_connection_config() -> TeradataConnectionConfig:
    env_data = get_env_data()
    return TeradataConnectionConfig(
        access_config=TeradataAccessConfig(password=env_data.password.get_secret_value()),
        host=env_data.host,
        user=env_data.user,
        database=env_data.database,
    )


def read_schema(filename: str, new_table_name: str, template_name: str) -> str:
    """Load a DDL file and point it at a per-run table, the way the connector does its own."""
    path = Path(env_setup_path / "sql" / "teradata" / "destination" / filename)
    # Strip before matching: an indented `--` would otherwise survive the filter and comment out
    # the rest of the joined statement. Join with a space for the same reason.
    lines = [line for line in path.read_text().splitlines() if not line.strip().startswith("--")]
    body = " ".join(line.strip() for line in lines)
    body = body.replace(f'"{template_name}"', f'"{new_table_name}"', 1).rstrip(";")
    # A missed substitution would issue CREATE TABLE against the shared template name, in a
    # database other runs are using.
    assert new_table_name in body, f"table name substitution failed for {filename}"
    return body


@contextmanager
def get_cursor() -> Generator[Any, None, None]:
    """Every driver call in this file goes through here, so setup and teardown are redacted too."""
    try:
        with get_connection_config().get_cursor() as cursor:
            yield cursor
    except Exception as e:
        # Anything the test body raised (an assertion, say) passes through untouched.
        if not _is_teradata_driver_error(e):
            raise
        raise TeradataTestError(e) from None


def drop_table(table_name: str) -> None:
    """Drop a per-run table, tolerating its absence.

    Teradata has no DROP TABLE IF EXISTS, so a table the test never created (the connector's
    auto-create failing, for instance) would otherwise report a second, unrelated failure on top
    of the real one.
    """
    try:
        with get_cursor() as cursor:
            logger.info(f"dropping table: {table_name}")
            cursor.execute(f'DROP TABLE "{table_name}"')
    except TeradataTestError as e:
        if e.code != TABLE_DOES_NOT_EXIST_CODE:
            raise
        logger.info(f"table {table_name} was already gone")


def random_table_name(prefix: str) -> str:
    """Randomise the table name: a ClearScape instance is shared and persistent, so unlike the
    docker-compose connectors here there is no fresh container per test to provide isolation.
    """
    return f"{prefix}_{str(uuid4())[:8]}"


@pytest.fixture
def destination_table() -> Generator[str, None, None]:
    """A table name the connector is expected to create for itself, dropped afterwards.

    Deliberately NOT pre-created. `create_destination()` returns early at its `DBC.TablesV` probe
    when the table already exists, so pre-creating it means the connector's own DDL asset is never
    read and never run, and the fixture becomes a copy of that asset with no drift guard.
    """
    table_name = random_table_name("elements")
    try:
        yield table_name
    finally:
        drop_table(table_name)


@pytest.fixture
def strict_table() -> Generator[str, None, None]:
    """The refusing fixture. This one has no connector-side equivalent, so the test creates it."""
    table_name = random_table_name("elements_strict")
    with get_cursor() as cursor:
        logger.info(f"creating table: {table_name}")
        cursor.execute(read_schema("schema_strict.sql", table_name, "elements_strict"))
    try:
        yield table_name
    finally:
        drop_table(table_name)


def get_uploader(table_name: str) -> TeradataUploader:
    return TeradataUploader(
        connection_config=get_connection_config(),
        upload_config=TeradataUploaderConfig(table_name=table_name),
    )


def mock_file_data(filename: str) -> FileData:
    return FileData(
        identifier="mock file data",
        connector_type=CONNECTOR_TYPE,
        source_identifiers=SourceIdentifiers(filename=filename, fullpath=filename),
    )


def count_rows(table_name: str) -> int:
    with get_cursor() as cursor:
        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        return cursor.fetchone()[0]


def fetch_row(table_name: str, row_id: str) -> tuple:
    with get_cursor() as cursor:
        cursor.execute(f'SELECT "text", "metadata" FROM "{table_name}" WHERE "id" = ?', [row_id])
        row = cursor.fetchone()
    assert row is not None, f"no row with the staged id landed in {table_name}"
    return row


def provoke_raw_driver_error(table_name: str, rows: list[tuple]) -> Exception:
    """Run an equivalent INSERT through a bare cursor and return the driver's own exception.

    The redaction assertions are only worth something against a message teradatasql actually
    produced: a hand-written string proves nothing about what the driver embeds, and there is no
    captured real one anywhere in this repo. Returned for comparison, never printed.
    """
    columns = ",".join(f'"{column}"' for column in STRICT_COLUMNS)
    placeholders = ",".join("?" for _ in STRICT_COLUMNS)
    statement = f'INSERT INTO "{table_name}" ({columns}) VALUES({placeholders})'
    with get_cursor() as cursor:
        try:
            cursor.executemany(statement, rows)
        except Exception as e:
            if not _is_teradata_driver_error(e):
                raise
            return e
    raise AssertionError(
        f"the provocation was ACCEPTED by {table_name}: the fixture no longer refuses it, so "
        "there is no rejection to classify. That is a different failure from the classification "
        "being wrong, and it is the one to chase first."
    )


def assert_classified_as_user_error(
    error: UserError, code: int, table_name: str, raw: Exception
) -> None:
    """The three things a rejection owes the user, and the one thing it must not leak.

    `raw` is the driver's own exception for the same provocation, and it is what makes the
    redaction half mean anything. `UserError`'s message is built from an int code, a fixed map
    descriptor and the table name, so scanning it for a password can never fail on its own.
    Asserting that a secret IS in the driver's text and IS NOT in ours is a real comparison, and
    it says which of the two halves moved.
    """
    message = str(error)
    raw_text = str(raw)
    env_data = get_env_data()

    assert f"Teradata error {code}" in message, message
    assert error.status_code == 422
    # It is a rejected record, not an unreachable host.
    assert "Failed to connect" not in message
    assert table_name in message  # the table IS deliberately named; it is the user's own

    # The server returns the code in the shape the classifier reads, and the last-match rule in
    # `_extract_teradata_error_code` picks the server's code rather than a batch-level tag the
    # driver may have appended after it.
    assert _extract_teradata_error_code(raw) == code

    # Compared as booleans and reported by name: a failure here must not print the driver text
    # or the password it carries.
    secrets = {
        "host": env_data.host,
        "user": env_data.user,
        "password": env_data.password.get_secret_value(),
    }
    in_driver_text = sorted(name for name, value in secrets.items() if value in raw_text)
    assert in_driver_text, (
        "the driver message embedded none of host/user/password, so the premise behind this "
        "connector's redaction has changed; re-check it before trusting the rest of this test"
    )
    leaked = sorted(name for name, value in secrets.items() if value in message)
    assert not leaked, f"the user-facing message leaked: {', '.join(leaked)}"
    embeds_driver_text = raw_text in message
    assert not embeds_driver_text, "the user-facing message embeds the driver's own text"


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
@requires_env(*REQUIRED_ENV)
def test_teradata_destination_upload(
    upload_file: Path, temp_dir: Path, destination_table: str
) -> None:
    """Staged records land in the table the connector creates, with their content intact.

    The stager is `metadata_as_json=True` because that is the pairing the connector documents:
    `create_destination()` builds the seven-column JSON-blob table and says twice that the stager
    must match. With the default flat stager, `_fit_to_schema` drops six staged columns
    (`orig_elements` among them) and null-fills `metadata`, and a row count never notices.

    Re-uploading the same record is part of the test: it is the only coverage
    `delete_by_record_id`'s row-removal semantics get, and it follows `test_postgres.py`.
    """
    file_data = mock_file_data(upload_file.name)
    stager = TeradataUploadStager(
        upload_stager_config=TeradataUploadStagerConfig(metadata_as_json=True)
    )
    staged_path = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=temp_dir,
        output_filename=upload_file.name,
    )
    staged = json.loads(staged_path.read_text())
    expected = len(staged)
    sample = next(record for record in staged if record.get("text"))

    uploader = get_uploader(destination_table)
    uploader.precheck()
    uploader.run(path=staged_path, file_data=file_data)

    assert count_rows(destination_table) == expected
    text, metadata = fetch_row(destination_table, sample["id"])
    assert text == sample["text"]
    # The JSON blob is the branch under test: metadata has to arrive as one parseable value.
    stored_metadata = json.loads(metadata) if isinstance(metadata, str) else metadata
    assert stored_metadata == json.loads(sample["metadata"])

    uploader.run(path=staged_path, file_data=file_data)
    assert count_rows(destination_table) == expected, "re-upload appended instead of replacing"


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
@requires_env(*REQUIRED_ENV)
@needs_widened_code_map
def test_teradata_destination_reports_a_duplicate_key_as_the_users_fault(
    strict_table: str,
) -> None:
    """Two rows sharing an id, against a UNIQUE PRIMARY INDEX, in one INSERT batch.

    Chosen over a bad-value provocation because the connector cannot sanitise it away: the
    rows are individually valid and only collide at the server. This is the assertion that
    needs a real Teradata to mean anything, because what is under test is that the server
    returns 2801 in the format the classifier reads.
    """
    shared_id = str(uuid4())
    df = pd.DataFrame(
        [
            dict(zip(STRICT_COLUMNS, (shared_id, "r1", "e1", "one", "Text"))),
            dict(zip(STRICT_COLUMNS, (shared_id, "r1", "e2", "two", "Text"))),
        ]
    )
    uploader = get_uploader(strict_table)

    with pytest.raises(UserError) as excinfo:
        uploader.upload_dataframe(df=df, file_data=mock_file_data("duplicate.json"))

    # No row-count post-condition: the connector documents that batches commit independently, so
    # whether the first of two colliding rows survives a 2801 depends on teradatasql array-insert
    # semantics nobody here has observed. Worth asserting once someone has.
    raw_id = str(uuid4())
    raw = provoke_raw_driver_error(
        strict_table,
        [
            (raw_id, "r2", "e1", "one", "Text"),
            (raw_id, "r2", "e2", "two", "Text"),
        ],
    )
    assert_classified_as_user_error(excinfo.value, DUPLICATE_KEY_CODE, strict_table, raw)


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
@requires_env(*REQUIRED_ENV)
@needs_widened_code_map
def test_teradata_destination_reports_an_untranslatable_character_as_the_users_fault(
    strict_table: str,
) -> None:
    """A character with no representation in the column's LATIN character set.

    The counterpart to the duplicate-key case: that one is refused on a constraint, this one
    on the content of a value, and both must read as the user's to fix rather than as a
    network problem.

    Unverified against a real server: `get_connection()` issues
    `SET SESSION CHARACTER SET UNICODE PASS THROUGH ON` on every connection, and whether that
    changes or suppresses 6706 for this value is not known. If it suppresses it, the raw
    provocation below fails with "the provocation was ACCEPTED", which is a different message
    from the classification being wrong, so the two are distinguishable in the output.
    """
    df = pd.DataFrame(
        [dict(zip(STRICT_COLUMNS, (str(uuid4()), "r1", "e1", NON_LATIN_TEXT, "Text")))]
    )
    uploader = get_uploader(strict_table)

    with pytest.raises(UserError) as excinfo:
        uploader.upload_dataframe(df=df, file_data=mock_file_data("untranslatable.json"))

    # One row, refused: nothing may be left behind.
    assert count_rows(strict_table) == 0

    raw = provoke_raw_driver_error(
        strict_table, [(str(uuid4()), "r2", "e1", NON_LATIN_TEXT, "Text")]
    )
    assert_classified_as_user_error(excinfo.value, UNTRANSLATABLE_CHARACTER_CODE, strict_table, raw)


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
def test_teradata_stager(upload_file: Path, temp_dir: Path) -> None:
    """Needs no server, so it is not gated: the stager is pure transformation."""
    stager_validation(
        configs=StagerValidationConfigs(test_id=CONNECTOR_TYPE, expected_count=22),
        input_file=upload_file,
        stager=TeradataUploadStager(),
        tmp_dir=temp_dir,
    )
