import json
import os
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import NotFound

from test.integration.connectors.utils.constants import DESTINATION_TAG, SOURCE_TAG
from test.integration.connectors.utils.validation import (
    ValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.databricks.volumes_native import (
    CONNECTOR_TYPE,
    DatabricksNativeVolumesAccessConfig,
    DatabricksNativeVolumesConnectionConfig,
    DatabricksNativeVolumesDownloader,
    DatabricksNativeVolumesDownloaderConfig,
    DatabricksNativeVolumesIndexer,
    DatabricksNativeVolumesIndexerConfig,
    DatabricksNativeVolumesUploader,
    DatabricksNativeVolumesUploaderConfig,
)


@dataclass
class EnvData:
    host: str
    client_id: str
    client_secret: str
    catalog: str

    def get_connection_config(self) -> DatabricksNativeVolumesConnectionConfig:
        return DatabricksNativeVolumesConnectionConfig(
            host=self.host,
            access_config=DatabricksNativeVolumesAccessConfig(
                client_id=self.client_id,
                client_secret=self.client_secret,
            ),
        )


def get_env_data() -> EnvData:
    return EnvData(
        host=os.environ["DATABRICKS_HOST"],
        client_id=os.environ["DATABRICKS_CLIENT_ID"],
        client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
        catalog=os.environ["DATABRICKS_CATALOG"],
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
@requires_env(
    "DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET", "DATABRICKS_CATALOG"
)
async def test_volumes_native_source():
    env_data = get_env_data()
    indexer_config = DatabricksNativeVolumesIndexerConfig(
        recursive=True,
        volume="test-platform",
        volume_path="databricks-volumes-test-input",
        catalog=env_data.catalog,
    )
    connection_config = env_data.get_connection_config()
    with tempfile.TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        download_config = DatabricksNativeVolumesDownloaderConfig(download_dir=tempdir_path)
        indexer = DatabricksNativeVolumesIndexer(
            connection_config=connection_config, index_config=indexer_config
        )
        downloader = DatabricksNativeVolumesDownloader(
            connection_config=connection_config, download_config=download_config
        )
        await source_connector_validation(
            indexer=indexer,
            downloader=downloader,
            configs=ValidationConfigs(
                test_id="databricks_volumes_native",
                expected_num_files=1,
            ),
        )


def _get_volume_path(catalog: str, volume: str, volume_path: str):
    return f"/Volumes/{catalog}/default/{volume}/{volume_path}"


@contextmanager
def databricks_destination_context(env_data: EnvData, volume: str, volume_path) -> WorkspaceClient:
    client = WorkspaceClient(
        host=env_data.host, client_id=env_data.client_id, client_secret=env_data.client_secret
    )
    try:
        yield client
    finally:
        # Cleanup
        try:
            for file in client.files.list_directory_contents(
                directory_path=_get_volume_path(env_data.catalog, volume, volume_path)
            ):
                client.files.delete(file.path)
            client.files.delete_directory(_get_volume_path(env_data.catalog, volume, volume_path))
        except NotFound:
            # Directory was never created, don't need to delete
            pass


def validate_upload(client: WorkspaceClient, catalog: str, volume: str, volume_path: str):
    files = list(
        client.files.list_directory_contents(
            directory_path=_get_volume_path(catalog, volume, volume_path)
        )
    )

    assert len(files) == 1

    resp = client.files.download(files[0].path)
    data = json.loads(resp.contents.read())

    assert len(data) == 22
    element_types = {v["type"] for v in data}
    assert len(element_types) == 1
    assert "CompositeElement" in element_types


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG)
@requires_env(
    "DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET", "DATABRICKS_CATALOG"
)
async def test_volumes_native_destination(upload_file: Path):
    env_data = get_env_data()
    volume_path = f"databricks-volumes-test-output-{uuid.uuid4()}"
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data",
    )
    with databricks_destination_context(
        volume="test-platform", volume_path=volume_path, env_data=env_data
    ) as workspace_client:
        connection_config = env_data.get_connection_config()
        uploader = DatabricksNativeVolumesUploader(
            connection_config=connection_config,
            upload_config=DatabricksNativeVolumesUploaderConfig(
                volume="test-platform",
                volume_path=volume_path,
                catalog=env_data.catalog,
            ),
        )
        if uploader.is_async():
            await uploader.run_async(path=upload_file, file_data=file_data)
        else:
            uploader.run(path=upload_file, file_data=file_data)

        validate_upload(
            client=workspace_client,
            catalog=env_data.catalog,
            volume="test-platform",
            volume_path=volume_path,
        )
