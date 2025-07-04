import multiprocessing
import os
from pathlib import Path

import pytest
from deltalake import DeltaTable
from fsspec import get_filesystem_class
from pydantic import Secret

from test.integration.connectors.utils.constants import DESTINATION_TAG, SQL_TAG
from test.integration.utils import requires_env
from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.processes.connectors.delta_table import (
    CONNECTOR_TYPE,
    DeltaTableAccessConfig,
    DeltaTableConnectionConfig,
    DeltaTableUploader,
    DeltaTableUploaderConfig,
    DeltaTableUploadStager,
    DeltaTableUploadStagerConfig,
)

multiprocessing.set_start_method("spawn")


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
async def test_delta_table_destination_local(upload_file: Path, temp_dir: Path):
    destination_path = str(temp_dir)
    connection_config = DeltaTableConnectionConfig(
        access_config=Secret(DeltaTableAccessConfig()),
        table_uri=destination_path,
    )
    stager_config = DeltaTableUploadStagerConfig()
    stager = DeltaTableUploadStager(upload_stager_config=stager_config)

    mock_file_data = FileData(
        identifier="mock file data",
        connector_type=CONNECTOR_TYPE,
        source_identifiers=SourceIdentifiers(
            filename=upload_file.name,
            fullpath=upload_file.name,
        ),
    )

    new_upload_file = stager.run(
        elements_filepath=upload_file,
        file_data=mock_file_data,
        output_dir=temp_dir,
        output_filename=upload_file.name,
    )

    upload_config = DeltaTableUploaderConfig()
    uploader = DeltaTableUploader(connection_config=connection_config, upload_config=upload_config)
    file_data = FileData(
        source_identifiers=SourceIdentifiers(
            fullpath=upload_file.name, filename=new_upload_file.name
        ),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data",
    )

    if uploader.is_async():
        await uploader.run_async(path=new_upload_file, file_data=file_data)
    else:
        uploader.run(path=new_upload_file, file_data=file_data)
    delta_table = DeltaTable(table_uri=destination_path)
    df = delta_table.to_pandas()

    EXPECTED_COLUMNS = 11
    EXPECTED_ROWS = 22
    assert len(df) == EXPECTED_ROWS, (
        f"Number of rows in table vs expected: {len(df)}/{EXPECTED_ROWS}"
    )
    assert len(df.columns) == EXPECTED_COLUMNS, (
        f"Number of columns in table vs expected: {len(df.columns)}/{EXPECTED_COLUMNS}"
    )


def get_aws_credentials() -> dict:
    access_key = os.getenv("S3_INGEST_TEST_ACCESS_KEY", None)
    assert access_key
    secret_key = os.getenv("S3_INGEST_TEST_SECRET_KEY", None)
    assert secret_key
    return {
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_REGION": "us-east-2",
    }


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
@requires_env("S3_INGEST_TEST_ACCESS_KEY", "S3_INGEST_TEST_SECRET_KEY")
async def test_delta_table_destination_s3(upload_file: Path, temp_dir: Path):
    aws_credentials = get_aws_credentials()
    s3_bucket = "s3://utic-platform-test-destination"
    destination_path = f"{s3_bucket}/destination/test"
    connection_config = DeltaTableConnectionConfig(
        access_config=Secret(
            DeltaTableAccessConfig(
                aws_access_key_id=aws_credentials["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=aws_credentials["AWS_SECRET_ACCESS_KEY"],
            )
        ),
        aws_region=aws_credentials["AWS_REGION"],
        table_uri=destination_path,
    )
    stager_config = DeltaTableUploadStagerConfig()
    stager = DeltaTableUploadStager(upload_stager_config=stager_config)

    mock_file_data = FileData(
        identifier="mock file data",
        connector_type=CONNECTOR_TYPE,
        source_identifiers=SourceIdentifiers(
            filename=upload_file.name,
            fullpath=upload_file.name,
        ),
    )

    new_upload_file = stager.run(
        elements_filepath=upload_file,
        file_data=mock_file_data,
        output_dir=temp_dir,
        output_filename=upload_file.name,
    )

    upload_config = DeltaTableUploaderConfig()
    uploader = DeltaTableUploader(connection_config=connection_config, upload_config=upload_config)
    file_data = FileData(
        source_identifiers=SourceIdentifiers(
            fullpath=upload_file.name, filename=new_upload_file.name
        ),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data",
    )

    try:
        uploader.precheck()
        if uploader.is_async():
            await uploader.run_async(path=new_upload_file, file_data=file_data)
        else:
            uploader.run(path=new_upload_file, file_data=file_data)
        delta_table = DeltaTable(table_uri=destination_path, storage_options=aws_credentials)
        df = delta_table.to_pandas()

        EXPECTED_COLUMNS = 11
        EXPECTED_ROWS = 22
        assert len(df) == EXPECTED_ROWS, (
            f"Number of rows in table vs expected: {len(df)}/{EXPECTED_ROWS}"
        )
        assert len(df.columns) == EXPECTED_COLUMNS, (
            f"Number of columns in table vs expected: {len(df.columns)}/{EXPECTED_COLUMNS}"
        )
    finally:
        s3fs = get_filesystem_class("s3")(
            key=aws_credentials["AWS_ACCESS_KEY_ID"],
            secret=aws_credentials["AWS_SECRET_ACCESS_KEY"],
        )
        s3fs.rm(path=destination_path, recursive=True)


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
@requires_env("S3_INGEST_TEST_ACCESS_KEY", "S3_INGEST_TEST_SECRET_KEY")
async def test_delta_table_destination_s3_bad_creds(upload_file: Path, temp_dir: Path):
    aws_credentials = {
        "AWS_ACCESS_KEY_ID": "bad key",
        "AWS_SECRET_ACCESS_KEY": "bad secret",
        "AWS_REGION": "us-east-2",
    }
    s3_bucket = "s3://utic-platform-test-destination"
    destination_path = f"{s3_bucket}/destination/test"
    connection_config = DeltaTableConnectionConfig(
        access_config=Secret(
            DeltaTableAccessConfig(
                aws_access_key_id=aws_credentials["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=aws_credentials["AWS_SECRET_ACCESS_KEY"],
            )
        ),
        aws_region=aws_credentials["AWS_REGION"],
        table_uri=destination_path,
    )
    stager_config = DeltaTableUploadStagerConfig()
    stager = DeltaTableUploadStager(upload_stager_config=stager_config)

    mock_file_data = FileData(
        identifier="mock file data",
        connector_type=CONNECTOR_TYPE,
        source_identifiers=SourceIdentifiers(
            filename=upload_file.name,
            fullpath=upload_file.name,
        ),
    )

    new_upload_file = stager.run(
        elements_filepath=upload_file,
        file_data=mock_file_data,
        output_dir=temp_dir,
        output_filename=upload_file.name,
    )

    upload_config = DeltaTableUploaderConfig()
    uploader = DeltaTableUploader(connection_config=connection_config, upload_config=upload_config)
    file_data = FileData(
        source_identifiers=SourceIdentifiers(
            fullpath=upload_file.name, filename=new_upload_file.name
        ),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data",
    )

    with pytest.raises(Exception) as excinfo:
        if uploader.is_async():
            await uploader.run_async(path=new_upload_file, file_data=file_data)
        else:
            uploader.run(path=new_upload_file, file_data=file_data)

    assert "403 Forbidden" in str(excinfo.value), f"Exception message did not match: {str(excinfo)}"
