import os
from pathlib import Path
from typing import Optional

import lancedb
import pandas as pd
import pytest
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


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG)
@pytest.mark.parametrize("target", ["local", "s3", "gcs", "az"])
async def test_lancedb_destination(upload_file: Path, target: str, tmp_path: Path) -> None:
    assert tmp_path.exists()
    assert tmp_path.is_dir()
    uri = _get_uri(target, tmp_path)
    connection = await lancedb.connect_async(
        uri=uri,
        storage_options={
            "aws_access_key_id": os.getenv("S3_INGEST_TEST_ACCESS_KEY"),
            "aws_secret_access_key": os.getenv("S3_INGEST_TEST_SECRET_KEY"),
            "google_service_account_key": os.getenv("GCP_INGEST_SERVICE_KEY"),
            "azure_storage_account_name": os.getenv("AZURE_STORAGE_ACCOUNT_NAME"),
            "azure_storage_account_key": os.getenv("AZURE_STORAGE_ACCOUNT_KEY"),
        },
    )
    await connection.create_table(name=TABLE_NAME, schema=TableSchema, mode="overwrite")

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
    try:
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
    finally:
        await connection.drop_database()


def _get_uri(target: str, tmp_path: Path) -> str:
    if target == "local":
        return str(tmp_path / DATABASE_NAME)

    if target == "s3":
        bucket = UPath(S3_BUCKET)
    if target == "gcs":
        bucket = UPath(GS_BUCKET)
    if target == "az":
        bucket = UPath(AZURE_BUCKET)
    return str(bucket / "destination" / "lancedb" / DATABASE_NAME)
