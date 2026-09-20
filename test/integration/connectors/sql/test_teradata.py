"""Live integration coverage for the Teradata SQL destination.

Covers the upload AND two rejections. The rejections are the point: a row the server refuses is
classified rather than written, and it is the classification that has been wrong, so a row-count
assertion alone would stay green through the defect this file follows.

DEPENDS ON THE WIDENED CODE MAP. The rejection tests assert `UserError`, which needs 2801 and 6706
in `_USER_FAULT_TERADATA_CODES`. They are added by the PR that widens that map; until it lands
both provocations raise `DestinationConnectionError` instead. Nothing goes red in the meantime
(these skip without credentials and CI never runs them), but a runner who supplies credentials
early will see the two rejection tests fail, and the map is the reason.

RUNNING IT. Not wired into CI, following `test_databricks_delta_tables.py` in this directory.
Teradata has no container image and no emulator, and the only free endpoint is a 60-day trial
(Teradata's ClearScape Analytics Experience), so a credential in CI secrets would rot and then
fail as an auth error rather than a skip.

    export TERADATA_HOST=...        # host or IP; the driver uses port 1025
    export TERADATA_USER=...
    export TERADATA_PASSWORD=...
    export TERADATA_DATABASE=...    # a database the user may CREATE TABLE in
    uv run pytest test/integration/connectors/sql/test_teradata.py -v

Never assert on or print the raw teradatasql message: the Go driver appends connection text that
embeds host, user and password. The rejection tests assert that text is ABSENT from what the user
sees, so against a real server they double as a leak check.
"""

import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Generator
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
from unstructured_ingest.error import UserError
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connectors.sql.teradata import (
    CONNECTOR_TYPE,
    TeradataAccessConfig,
    TeradataConnectionConfig,
    TeradataUploader,
    TeradataUploaderConfig,
    TeradataUploadStager,
)

REQUIRED_ENV = ("TERADATA_HOST", "TERADATA_USER", "TERADATA_PASSWORD", "TERADATA_DATABASE")

# Codes the server raises for the two rejections provoked below. Both are in
# `_USER_FAULT_TERADATA_CODES`, so both must surface as UserError naming the code.
DUPLICATE_KEY_CODE = 2801
UNTRANSLATABLE_CHARACTER_CODE = 6706

# A character with no LATIN representation, for the 6706 provocation. Kept as an escape rather
# than a literal so the file stays ASCII and the intent is legible.
NON_LATIN_TEXT = "中文"


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
    lines = [line for line in path.read_text().splitlines() if not line.startswith("--")]
    body = "".join(line.strip() for line in lines)
    return body.replace(f'"{template_name}"', f'"{new_table_name}"', 1).rstrip(";")


@contextmanager
def get_cursor() -> Generator[object, None, None]:
    with get_connection_config().get_connection() as connection:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()


@contextmanager
def _table(filename: str, template_name: str) -> Generator[str, None, None]:
    """Create a per-run table and drop it afterwards.

    The name is randomised because a ClearScape instance is shared and persistent: unlike the
    docker-compose connectors here, there is no fresh container per test to provide isolation.
    """
    table_name = f"{template_name}_{str(uuid4())[:8]}"
    with get_cursor() as cursor:
        logger.info(f"creating table: {table_name}")
        cursor.execute(read_schema(filename, table_name, template_name))
    try:
        yield table_name
    finally:
        with get_cursor() as cursor:
            logger.info(f"dropping table: {table_name}")
            cursor.execute(f'DROP TABLE "{table_name}"')


@pytest.fixture
def destination_table() -> Generator[str, None, None]:
    with _table("schema.sql", "elements") as table_name:
        yield table_name


@pytest.fixture
def strict_table() -> Generator[str, None, None]:
    with _table("schema_strict.sql", "elements_strict") as table_name:
        yield table_name


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


def assert_safe_user_error(error: UserError, code: int, table_name: str) -> None:
    """The three things a rejection owes the user, and the one thing it must not leak."""
    message = str(error)
    assert f"Teradata error {code}" in message, message
    assert error.status_code == 422
    # It is a rejected record, not an unreachable host.
    assert "Failed to connect" not in message
    # Nothing the driver appended may cross: the host, the credentials it embeds in its
    # connection text, or the server's own interpolated message.
    assert get_env_data().host not in message
    assert get_env_data().user not in message
    assert get_env_data().password.get_secret_value() not in message
    assert "CONNECTION=" not in message
    assert table_name in message  # the table IS deliberately named; it is the user's own


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
@requires_env(*REQUIRED_ENV)
def test_teradata_destination_upload(
    upload_file: Path, temp_dir: Path, destination_table: str
) -> None:
    """The cheaper half: records staged and written land in the table."""
    file_data = mock_file_data(upload_file.name)
    stager = TeradataUploadStager()
    staged_path = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=temp_dir,
        output_filename=upload_file.name,
    )
    expected = len(json.loads(staged_path.read_text()))

    uploader = get_uploader(destination_table)
    uploader.precheck()
    uploader.run(path=staged_path, file_data=file_data)

    assert count_rows(destination_table) == expected


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
@requires_env(*REQUIRED_ENV)
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
            {"id": shared_id, "record_id": "r1", "element_id": "e1", "text": "one", "type": "Text"},
            {"id": shared_id, "record_id": "r1", "element_id": "e2", "text": "two", "type": "Text"},
        ]
    )
    uploader = get_uploader(strict_table)

    with pytest.raises(UserError) as excinfo:
        uploader.upload_dataframe(df=df, file_data=mock_file_data("duplicate.json"))

    assert_safe_user_error(excinfo.value, DUPLICATE_KEY_CODE, strict_table)


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
@requires_env(*REQUIRED_ENV)
def test_teradata_destination_reports_an_untranslatable_character_as_the_users_fault(
    strict_table: str,
) -> None:
    """A character with no representation in the column's LATIN character set.

    The counterpart to the duplicate-key case: that one is refused on a constraint, this one
    on the content of a value, and both must read as the user's to fix rather than as a
    network problem.
    """
    df = pd.DataFrame(
        [
            {
                "id": str(uuid4()),
                "record_id": "r1",
                "element_id": "e1",
                "text": NON_LATIN_TEXT,
                "type": "Text",
            }
        ]
    )
    uploader = get_uploader(strict_table)

    with pytest.raises(UserError) as excinfo:
        uploader.upload_dataframe(df=df, file_data=mock_file_data("untranslatable.json"))

    assert_safe_user_error(excinfo.value, UNTRANSLATABLE_CHARACTER_CODE, strict_table)


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
def test_teradata_stager(upload_file: Path, temp_dir: Path) -> None:
    """Needs no server, so it is not gated: the stager is pure transformation."""
    stager_validation(
        configs=StagerValidationConfigs(test_id=CONNECTOR_TYPE, expected_count=22),
        input_file=upload_file,
        stager=TeradataUploadStager(),
        tmp_dir=temp_dir,
    )
