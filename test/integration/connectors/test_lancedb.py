import os
from pathlib import Path
from typing import Literal, Optional

import lancedb
import pandas as pd
import pytest
import pytest_asyncio
from lancedb import AsyncConnection
from lancedb.pydantic import LanceModel, Vector
from upath import UPath

from test.integration.connectors.utils.constants import DESTINATION_TAG
from unstructured_ingest.v2.interfaces.file_data import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.lancedb import (
    CONNECTOR_TYPE,
    LanceDBAccessConfig,
    LanceDBConnectionConfig,
    LanceDBUploader,
    LanceDBUploaderConfig,
    LanceDBUploadStager,
)

DATABASE_NAME = "database"
TABLE_NAME = "elements"
DIMENSION = 384
NUMBER_EXPECTED_ROWS = 22
NUMBER_EXPECTED_COLUMNS = 9
S3_BUCKET = "s3://utic-ingest-test-fixtures/"
GS_BUCKET = "gs://utic-test-ingest-fixtures-output/"
AZURE_BUCKET = "az://utic-ingest-test-fixtures-output/"


class TableSchema(LanceModel):
    vector: Vector(DIMENSION)  # type: ignore
    text: Optional[str]
    element_id: Optional[str]
    text_as_html: Optional[str]
    file_type: Optional[str]
    filename: Optional[str]
    type: Optional[str]
    is_continuation: Optional[bool]
    page_number: Optional[int]


@pytest_asyncio.fixture
async def connection_with_uri(request, tmp_path: Path):
    uri = _get_uri(request.param, local_base_path=tmp_path)
    storage_options = {
        "aws_access_key_id": os.getenv("S3_INGEST_TEST_ACCESS_KEY"),
        "aws_secret_access_key": os.getenv("S3_INGEST_TEST_SECRET_KEY"),
        "google_service_account_key": os.getenv("GCP_INGEST_SERVICE_KEY"),
        "azure_storage_account_name": os.getenv("AZURE_STORAGE_ACCOUNT_NAME"),
        "azure_storage_account_key": os.getenv("AZURE_STORAGE_ACCOUNT_KEY"),
    }
    storage_options = {key: value for key, value in storage_options.items() if value is not None}
    connection = await lancedb.connect_async(
        uri=uri,
        storage_options=storage_options,
    )
    await connection.create_table(name=TABLE_NAME, schema=TableSchema, mode="overwrite")

    yield connection, uri

    await connection.drop_database()


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG)
@pytest.mark.parametrize("connection_with_uri", ["local", "s3", "gcs", "az"], indirect=True)
async def test_lancedb_destination(
    upload_file: Path,
    connection_with_uri: tuple[AsyncConnection, str],
    tmp_path: Path,
) -> None:
    connection, uri = connection_with_uri
    if uri.startswith("s3"):
        assert "S3_INGEST_TEST_ACCESS_KEY" in os.environ, "Missing S3_INGEST_TEST_ACCESS_KEY"
        assert "S3_INGEST_TEST_SECRET_KEY" in os.environ, "Missing S3_INGEST_TEST_SECRET_KEY"
    elif uri.startswith("gs"):
        assert "GCP_INGEST_SERVICE_KEY" in os.environ, "Missing GCP_INGEST_SERVICE_KEY"
    elif uri.startswith("az"):
        assert "AZURE_STORAGE_ACCOUNT_NAME" in os.environ, "Missing AZURE_STORAGE_ACCOUNT_NAME"
        assert "AZURE_STORAGE_ACCOUNT_KEY" in os.environ, "Missing AZURE_STORAGE_ACCOUNT_KEY"

    access_config = LanceDBAccessConfig(
        s3_access_key_id=os.getenv("S3_INGEST_TEST_ACCESS_KEY"),
        s3_secret_access_key=os.getenv("S3_INGEST_TEST_SECRET_KEY"),
        google_service_account_key=os.getenv("GCP_INGEST_SERVICE_KEY"),
        azure_storage_account_name=os.getenv("AZURE_STORAGE_ACCOUNT_NAME"),
        azure_storage_account_key=os.getenv("AZURE_STORAGE_ACCOUNT_KEY"),
    )
    connection_config = LanceDBConnectionConfig(
        access_config=access_config,
        uri=uri,
        table_name=TABLE_NAME,
    )
    stager = LanceDBUploadStager()
    uploader = LanceDBUploader(
        upload_config=LanceDBUploaderConfig(),
        connection_config=connection_config,
    )
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data",
    )
    staged_file_path = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file.name,
    )
    await uploader.run_async(path=staged_file_path, file_data=file_data)

    table = await connection.open_table(TABLE_NAME)
    table_df: pd.DataFrame = await table.to_pandas()

    assert len(table_df) == NUMBER_EXPECTED_ROWS
    assert len(table_df.columns) == NUMBER_EXPECTED_COLUMNS

    assert table_df["element_id"][0] == "2470d8dc42215b3d68413b55bf00fed2"
    assert table_df["type"][0] == "CompositeElement"
    assert table_df["filename"][0] == "DA-1p-with-duplicate-pages.pdf.json"
    assert table_df["text_as_html"][0] is None


def _get_uri(target: Literal["local", "s3", "gcs", "az"], local_base_path: Path) -> str:
    if target == "local":
        return str(local_base_path / DATABASE_NAME)
    if target == "s3":
        base_uri = UPath(S3_BUCKET)
    elif target == "gcs":
        base_uri = UPath(GS_BUCKET)
    elif target == "az":
        base_uri = UPath(AZURE_BUCKET)

    return str(base_uri / "destination" / "lancedb" / DATABASE_NAME)
