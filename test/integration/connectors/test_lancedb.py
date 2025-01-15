import os
from pathlib import Path
from typing import Literal, Union
from uuid import uuid4

import lancedb
import pandas as pd
import pyarrow as pa
import pytest
import pytest_asyncio
from lancedb import AsyncConnection
from upath import UPath

from test.integration.connectors.utils.constants import DESTINATION_TAG, VECTOR_DB_TAG
from unstructured_ingest.v2.constants import RECORD_ID_LABEL
from unstructured_ingest.v2.interfaces.file_data import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.lancedb.aws import (
    LanceDBAwsAccessConfig,
    LanceDBAwsConnectionConfig,
    LanceDBAwsUploader,
)
from unstructured_ingest.v2.processes.connectors.lancedb.azure import (
    LanceDBAzureAccessConfig,
    LanceDBAzureConnectionConfig,
    LanceDBAzureUploader,
)
from unstructured_ingest.v2.processes.connectors.lancedb.gcp import (
    LanceDBGCSAccessConfig,
    LanceDBGCSConnectionConfig,
    LanceDBGSPUploader,
)
from unstructured_ingest.v2.processes.connectors.lancedb.lancedb import (
    CONNECTOR_TYPE,
    LanceDBUploaderConfig,
    LanceDBUploadStager,
)
from unstructured_ingest.v2.processes.connectors.lancedb.local import (
    LanceDBLocalAccessConfig,
    LanceDBLocalConnectionConfig,
    LanceDBLocalUploader,
)

DATABASE_NAME = "database"
TABLE_NAME = "elements"
DIMENSION = 384
NUMBER_EXPECTED_ROWS = 22
S3_BUCKET = "s3://utic-ingest-test-fixtures/"
GS_BUCKET = "gs://utic-test-ingest-fixtures-output/"
AZURE_BUCKET = "az://utic-ingest-test-fixtures-output/"
REQUIRED_ENV_VARS = {
    "s3": ("S3_INGEST_TEST_ACCESS_KEY", "S3_INGEST_TEST_SECRET_KEY"),
    "gcs": ("GCP_INGEST_SERVICE_KEY",),
    "az": ("AZURE_DEST_CONNECTION_STR",),
    "local": (),
}

SCHEMA = pa.schema(
    [
        pa.field(RECORD_ID_LABEL, pa.string()),
        pa.field("vector", pa.list_(pa.float16(), DIMENSION)),
        pa.field("text", pa.string(), nullable=True),
        pa.field("type", pa.string(), nullable=True),
        pa.field("element_id", pa.string(), nullable=True),
        pa.field("metadata-text_as_html", pa.string(), nullable=True),
        pa.field("metadata-filetype", pa.string(), nullable=True),
        pa.field("metadata-filename", pa.string(), nullable=True),
        pa.field("metadata-languages", pa.list_(pa.string()), nullable=True),
        pa.field("metadata-is_continuation", pa.bool_(), nullable=True),
        pa.field("metadata-page_number", pa.int32(), nullable=True),
    ]
)
NUMBER_EXPECTED_COLUMNS = len(SCHEMA.names)


@pytest_asyncio.fixture
async def connection_with_uri(request, tmp_path: Path):
    target = request.param
    uri = _get_uri(target, local_base_path=tmp_path)

    unset_variables = [env for env in REQUIRED_ENV_VARS[target] if env not in os.environ]
    if unset_variables:
        pytest.skip(
            reason="Following required environment variables were not set: "
            + f"{', '.join(unset_variables)}"
        )

    storage_options = {
        "aws_access_key_id": os.getenv("S3_INGEST_TEST_ACCESS_KEY"),
        "aws_secret_access_key": os.getenv("S3_INGEST_TEST_SECRET_KEY"),
        "google_service_account_key": os.getenv("GCP_INGEST_SERVICE_KEY"),
    }
    azure_connection_string = os.getenv("AZURE_DEST_CONNECTION_STR")
    if azure_connection_string:
        storage_options.update(_parse_azure_connection_string(azure_connection_string))

    storage_options = {key: value for key, value in storage_options.items() if value is not None}
    connection = await lancedb.connect_async(
        uri=uri,
        storage_options=storage_options,
    )
    await connection.create_table(name=TABLE_NAME, schema=SCHEMA)

    yield connection, uri

    await connection.drop_database()


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
@pytest.mark.parametrize("connection_with_uri", ["local", "s3", "gcs", "az"], indirect=True)
async def test_lancedb_destination(
    upload_file: Path,
    connection_with_uri: tuple[AsyncConnection, str],
    tmp_path: Path,
) -> None:
    connection, uri = connection_with_uri
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock-file-data",
    )
    stager = LanceDBUploadStager()
    uploader = _get_uploader(uri)
    staged_file_path = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file.name,
    )

    await uploader.run_async(path=staged_file_path, file_data=file_data)

    # Test upload to empty table
    with await connection.open_table(TABLE_NAME) as table:
        table_df: pd.DataFrame = await table.to_pandas()

    assert len(table_df) == NUMBER_EXPECTED_ROWS
    assert len(table_df.columns) == NUMBER_EXPECTED_COLUMNS

    assert table_df[RECORD_ID_LABEL][0] == file_data.identifier
    assert table_df["element_id"][0] == "2470d8dc42215b3d68413b55bf00fed2"
    assert table_df["type"][0] == "CompositeElement"
    assert table_df["metadata-filename"][0] == "DA-1p-with-duplicate-pages.pdf.json"
    assert table_df["metadata-text_as_html"][0] is None

    # Test upload of the second file, rows should be appended
    file_data.identifier = "mock-file-data-2"
    staged_second_file_path = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=f"{upload_file.stem}-2{upload_file.suffix}",
    )
    await uploader.run_async(path=staged_second_file_path, file_data=file_data)
    with await connection.open_table(TABLE_NAME) as table:
        appended_table_df: pd.DataFrame = await table.to_pandas()
    assert len(appended_table_df) == 2 * NUMBER_EXPECTED_ROWS

    # Test re-upload of the first file, rows should be overwritten, not appended
    await uploader.run_async(path=staged_file_path, file_data=file_data)
    with await connection.open_table(TABLE_NAME) as table:
        overwritten_table_df: pd.DataFrame = await table.to_pandas()
    assert len(overwritten_table_df) == 2 * NUMBER_EXPECTED_ROWS


class TestPrecheck:
    @pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
    @pytest.mark.parametrize("connection_with_uri", ["local", "s3", "gcs", "az"], indirect=True)
    def test_succeeds(
        self,
        upload_file: Path,
        connection_with_uri: tuple[AsyncConnection, str],
        tmp_path: Path,
    ) -> None:
        _, uri = connection_with_uri
        uploader = _get_uploader(uri)
        uploader.precheck()


def _get_uri(target: Literal["local", "s3", "gcs", "az"], local_base_path: Path) -> str:
    if target == "local":
        return str(local_base_path / DATABASE_NAME)
    if target == "s3":
        base_uri = UPath(S3_BUCKET)
    elif target == "gcs":
        base_uri = UPath(GS_BUCKET)
    elif target == "az":
        base_uri = UPath(AZURE_BUCKET)

    return str(base_uri / "destination" / "lancedb" / str(uuid4()) / DATABASE_NAME)


def _get_uploader(
    uri: str,
) -> Union[LanceDBAzureUploader, LanceDBAzureUploader, LanceDBAwsUploader, LanceDBGSPUploader]:
    target = uri.split("://", maxsplit=1)[0] if uri.startswith(("s3", "az", "gs")) else "local"
    upload_config = LanceDBUploaderConfig(table_name=TABLE_NAME)
    if target == "az":
        azure_connection_string = os.getenv("AZURE_DEST_CONNECTION_STR")
        access_config_kwargs = _parse_azure_connection_string(azure_connection_string)
        return LanceDBAzureUploader(
            upload_config=upload_config,
            connection_config=LanceDBAzureConnectionConfig(
                access_config=LanceDBAzureAccessConfig(**access_config_kwargs),
                uri=uri,
            ),
        )

    elif target == "s3":
        return LanceDBAwsUploader(
            upload_config=upload_config,
            connection_config=LanceDBAwsConnectionConfig(
                access_config=LanceDBAwsAccessConfig(
                    aws_access_key_id=os.getenv("S3_INGEST_TEST_ACCESS_KEY"),
                    aws_secret_access_key=os.getenv("S3_INGEST_TEST_SECRET_KEY"),
                ),
                uri=uri,
            ),
        )
    elif target == "gs":
        return LanceDBGSPUploader(
            upload_config=upload_config,
            connection_config=LanceDBGCSConnectionConfig(
                access_config=LanceDBGCSAccessConfig(
                    google_service_account_key=os.getenv("GCP_INGEST_SERVICE_KEY")
                ),
                uri=uri,
            ),
        )
    else:
        return LanceDBLocalUploader(
            upload_config=upload_config,
            connection_config=LanceDBLocalConnectionConfig(
                access_config=LanceDBLocalAccessConfig(),
                uri=uri,
            ),
        )


def _parse_azure_connection_string(
    connection_str: str,
) -> dict[Literal["azure_storage_account_name", "azure_storage_account_key"], str]:
    parameters = dict(keyvalue.split("=", maxsplit=1) for keyvalue in connection_str.split(";"))
    return {
        "azure_storage_account_name": parameters.get("AccountName"),
        "azure_storage_account_key": parameters.get("AccountKey"),
    }
