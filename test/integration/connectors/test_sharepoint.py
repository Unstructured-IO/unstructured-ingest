import os

import pytest

from test.integration.connectors.utils.constants import BLOB_STORAGE_TAG, SOURCE_TAG
from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
    source_filedata_display_name_set_check,
)
from test.integration.utils import requires_env
from unstructured_ingest.processes.connectors.sharepoint import (
    CONNECTOR_TYPE,
    SharepointAccessConfig,
    SharepointConnectionConfig,
    SharepointDownloader,
    SharepointDownloaderConfig,
    SharepointIndexer,
    SharepointIndexerConfig,
)


def sharepoint_config():
    class SharepointTestConfig:
        def __init__(self):
            self.client_id = os.environ["SHAREPOINT_CLIENT_ID"]
            self.client_cred = os.environ["SHAREPOINT_CRED"]
            self.tenant = os.environ["MS_TENANT_ID"]

    return SharepointTestConfig()


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, BLOB_STORAGE_TAG)
@requires_env("SHAREPOINT_CLIENT_ID", "SHAREPOINT_CRED", "MS_TENANT_ID", "MS_USER_PNAME")
async def test_sharepoint_source(temp_dir):
    site = "https://unstructuredio.sharepoint.com/sites/utic-platform-test-source"
    config = sharepoint_config()

    # Create connection and indexer configurations
    access_config = SharepointAccessConfig(client_cred=config.client_cred)
    connection_config = SharepointConnectionConfig(
        client_id=config.client_id,
        site=site,
        tenant=config.tenant,
        access_config=access_config,
    )
    index_config = SharepointIndexerConfig(recursive=True)

    download_config = SharepointDownloaderConfig(download_dir=temp_dir)

    # Instantiate indexer and downloader
    indexer = SharepointIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    downloader = SharepointDownloader(
        connection_config=connection_config,
        download_config=download_config,
    )

    # Run the source connector validation
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="sharepoint1",
            expected_num_files=4,
            validate_downloaded_files=True,
            exclude_fields_extend=[
                "metadata.date_created",
                "metadata.date_modified",
                "additional_metadata.LastModified",
                "additional_metadata.@microsoft.graph.downloadUrl",
            ],
            predownload_file_data_check=source_filedata_display_name_set_check,
            postdownload_file_data_check=source_filedata_display_name_set_check,
        ),
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, BLOB_STORAGE_TAG)
@requires_env("SHAREPOINT_CLIENT_ID", "SHAREPOINT_CRED", "MS_TENANT_ID", "MS_USER_PNAME")
async def test_sharepoint_source_with_path(temp_dir):
    site = "https://unstructuredio.sharepoint.com/sites/utic-platform-test-source"
    config = sharepoint_config()

    # Create connection and indexer configurations
    access_config = SharepointAccessConfig(client_cred=config.client_cred)
    connection_config = SharepointConnectionConfig(
        client_id=config.client_id,
        site=site,
        tenant=config.tenant,
        access_config=access_config,
    )
    index_config = SharepointIndexerConfig(recursive=True, path="Folder1")

    download_config = SharepointDownloaderConfig(download_dir=temp_dir)

    # Instantiate indexer and downloader
    indexer = SharepointIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    downloader = SharepointDownloader(
        connection_config=connection_config,
        download_config=download_config,
    )

    # Run the source connector validation
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="sharepoint2",
            expected_num_files=2,
            validate_downloaded_files=True,
            exclude_fields_extend=[
                "metadata.date_created",
                "metadata.date_modified",
                "additional_metadata.LastModified",
                "additional_metadata.@microsoft.graph.downloadUrl",
            ],
            predownload_file_data_check=source_filedata_display_name_set_check,
            postdownload_file_data_check=source_filedata_display_name_set_check,
        ),
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, BLOB_STORAGE_TAG)
@requires_env("SHAREPOINT_CLIENT_ID", "SHAREPOINT_CRED", "MS_TENANT_ID", "MS_USER_PNAME")
async def test_sharepoint_root_with_path(temp_dir):
    site = "https://unstructuredio.sharepoint.com/"
    config = sharepoint_config()

    # Create connection and indexer configurations
    access_config = SharepointAccessConfig(client_cred=config.client_cred)
    connection_config = SharepointConnectionConfig(
        client_id=config.client_id,
        site=site,
        tenant=config.tenant,
        access_config=access_config,
    )
    index_config = SharepointIndexerConfig(recursive=True, path="e2e-test-folder")

    download_config = SharepointDownloaderConfig(download_dir=temp_dir)

    # Instantiate indexer and downloader
    indexer = SharepointIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    downloader = SharepointDownloader(
        connection_config=connection_config,
        download_config=download_config,
    )

    # Run the source connector validation
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="sharepoint3",
            expected_num_files=2,
            validate_downloaded_files=True,
            exclude_fields_extend=[
                "metadata.date_created",
                "metadata.date_modified",
                "additional_metadata.LastModified",
                "additional_metadata.@microsoft.graph.downloadUrl",
            ],
            predownload_file_data_check=source_filedata_display_name_set_check,
            postdownload_file_data_check=source_filedata_display_name_set_check,
        ),
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, BLOB_STORAGE_TAG)
@requires_env("SHAREPOINT_CLIENT_ID", "SHAREPOINT_CRED", "MS_TENANT_ID", "MS_USER_PNAME")
async def test_sharepoint_shared_documents(temp_dir):
    site = "https://unstructuredio.sharepoint.com/sites/utic-platform-test-source"
    config = sharepoint_config()

    # Create connection and indexer configurations
    access_config = SharepointAccessConfig(client_cred=config.client_cred)
    connection_config = SharepointConnectionConfig(
        client_id=config.client_id,
        site=site,
        tenant=config.tenant,
        access_config=access_config,
    )
    index_config = SharepointIndexerConfig(recursive=True, path="Shared Documents")

    download_config = SharepointDownloaderConfig(download_dir=temp_dir)

    # Instantiate indexer and downloader
    indexer = SharepointIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    downloader = SharepointDownloader(
        connection_config=connection_config,
        download_config=download_config,
    )

    # Run the source connector validation
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="sharepoint4",
            expected_num_files=4,
            validate_downloaded_files=True,
            exclude_fields_extend=[
                "metadata.date_created",
                "metadata.date_modified",
                "additional_metadata.LastModified",
                "additional_metadata.@microsoft.graph.downloadUrl",
            ],
            predownload_file_data_check=source_filedata_display_name_set_check,
            postdownload_file_data_check=source_filedata_display_name_set_check,
        ),
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, BLOB_STORAGE_TAG)
@requires_env("SHAREPOINT_CLIENT_ID", "SHAREPOINT_CRED", "MS_TENANT_ID", "MS_USER_PNAME")
async def test_sharepoint_library(temp_dir):
    site = "https://unstructuredio.sharepoint.com/sites/utic-platform-test-source"
    library = "Documents2"
    config = sharepoint_config()

    # Create connection and indexer configurations
    access_config = SharepointAccessConfig(client_cred=config.client_cred)
    connection_config = SharepointConnectionConfig(
        client_id=config.client_id,
        site=site,
        library=library,
        tenant=config.tenant,
        access_config=access_config,
    )
    index_config = SharepointIndexerConfig(recursive=True)

    download_config = SharepointDownloaderConfig(download_dir=temp_dir)

    # Instantiate indexer and downloader
    indexer = SharepointIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    downloader = SharepointDownloader(
        connection_config=connection_config,
        download_config=download_config,
    )

    # Run the source connector validation
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="sharepoint5",
            expected_num_files=3,
            validate_downloaded_files=True,
            exclude_fields_extend=[
                "metadata.date_created",
                "metadata.date_modified",
                "additional_metadata.LastModified",
                "additional_metadata.@microsoft.graph.downloadUrl",
            ],
            predownload_file_data_check=source_filedata_display_name_set_check,
            postdownload_file_data_check=source_filedata_display_name_set_check,
        ),
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, BLOB_STORAGE_TAG)
@requires_env("SHAREPOINT_CLIENT_ID", "SHAREPOINT_CRED", "MS_TENANT_ID", "MS_USER_PNAME")
async def test_sharepoint_library_with_path(temp_dir):
    site = "https://unstructuredio.sharepoint.com/sites/utic-platform-test-source"
    library = "Documents2"
    config = sharepoint_config()

    # Create connection and indexer configurations
    access_config = SharepointAccessConfig(client_cred=config.client_cred)
    connection_config = SharepointConnectionConfig(
        client_id=config.client_id,
        site=site,
        library=library,
        tenant=config.tenant,
        access_config=access_config,
    )
    index_config = SharepointIndexerConfig(recursive=True, path="e2e-library-folder")

    download_config = SharepointDownloaderConfig(download_dir=temp_dir)

    # Instantiate indexer and downloader
    indexer = SharepointIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )
    downloader = SharepointDownloader(
        connection_config=connection_config,
        download_config=download_config,
    )

    # Run the source connector validation
    await source_connector_validation(
        indexer=indexer,
        downloader=downloader,
        configs=SourceValidationConfigs(
            test_id="sharepoint6",
            expected_num_files=1,
            validate_downloaded_files=True,
            exclude_fields_extend=[
                "metadata.date_created",
                "metadata.date_modified",
                "additional_metadata.LastModified",
                "additional_metadata.@microsoft.graph.downloadUrl",
            ],
            predownload_file_data_check=source_filedata_display_name_set_check,
            postdownload_file_data_check=source_filedata_display_name_set_check,
        ),
    )
