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


@pytest.fixture
def base_sharepoint_config():
    """Base SharePoint config for testing."""
    config = sharepoint_config()
    return {
        'client_id': config.client_id,
        'tenant': config.tenant,
        'site': "https://unstructuredai.sharepoint.com/sites/ingest-integration-test",
        'client_cred': config.client_cred
    }


@pytest.fixture
def indexer_factory(base_sharepoint_config):
    """Factory for creating SharePoint indexers with different configs."""
    def _create_indexer(client_cred=None, site=None, path=None):
        access_config = SharepointAccessConfig(
            client_cred=client_cred or base_sharepoint_config['client_cred']
        )
        connection_config = SharepointConnectionConfig(
            client_id=base_sharepoint_config['client_id'],
            site=site or base_sharepoint_config['site'],
            tenant=base_sharepoint_config['tenant'],
            access_config=access_config,
        )
        index_config = SharepointIndexerConfig(path=path)
        return SharepointIndexer(
            connection_config=connection_config,
            index_config=index_config,
        )
    return _create_indexer


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, BLOB_STORAGE_TAG)
@requires_env("SHAREPOINT_CLIENT_ID", "SHAREPOINT_CRED", "MS_TENANT_ID", "MS_USER_PNAME")
@pytest.mark.parametrize("error_scenario,expected_error", [
    ("invalid_creds", "UserAuthError"),
    ("nonexistent_site", "UserError"),
    ("invalid_path", "UserError"),
])
async def test_sharepoint_precheck_error_scenarios(indexer_factory, error_scenario, expected_error):
    """Parametrized test for different SharePoint precheck error scenarios."""
    from unstructured_ingest.errors_v2 import UserAuthError, UserError
    
    error_class_map = {
        "UserAuthError": UserAuthError,
        "UserError": UserError
    }
    
    expected_exception = error_class_map[expected_error]
    
    if error_scenario == "invalid_creds":
        indexer = indexer_factory(client_cred="invalid_creds")
    elif error_scenario == "nonexistent_site":
        indexer = indexer_factory(site="https://unstructuredai.sharepoint.com/sites/definitely-does-not-exist-12345")
    elif error_scenario == "invalid_path":
        indexer = indexer_factory(path="NonExistentFolder/SubFolder/DoesNotExist")
    else:
        pytest.fail(f"Unknown error scenario: {error_scenario}")
    
    with pytest.raises(expected_exception):
        indexer.precheck()


@pytest.fixture
def insufficient_perms_config():
    """Config for testing insufficient permissions."""
    from types import SimpleNamespace
    
    required_vars = [
        "SHAREPOINT_CLIENT_ID_INSUFFICIENT",
        "SHAREPOINT_CRED_INSUFFICIENT", 
        "MS_TENANT_ID_INSUFFICIENT"
    ]
    
    missing_vars = [var for var in required_vars if var not in os.environ]
    if missing_vars:
        pytest.skip(f"Missing environment variables: {missing_vars}")
    
    return SimpleNamespace(
        client_id=os.environ["SHAREPOINT_CLIENT_ID_INSUFFICIENT"],
        client_cred=os.environ["SHAREPOINT_CRED_INSUFFICIENT"],
        tenant=os.environ["MS_TENANT_ID_INSUFFICIENT"]
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG, BLOB_STORAGE_TAG)
@requires_env("MS_USER_PNAME")
async def test_sharepoint_precheck_insufficient_permissions(insufficient_perms_config):
    """Test precheck with credentials that have insufficient permissions."""
    from unstructured_ingest.errors_v2 import UserAuthError

    restricted_site = "https://unstructuredai.sharepoint.com/sites/ingest-integration-test"

    access_config = SharepointAccessConfig(client_cred=insufficient_perms_config.client_cred)
    connection_config = SharepointConnectionConfig(
        client_id=insufficient_perms_config.client_id,
        site=restricted_site,
        tenant=insufficient_perms_config.tenant,
        access_config=access_config,
    )
    index_config = SharepointIndexerConfig()

    indexer = SharepointIndexer(
        connection_config=connection_config,
        index_config=index_config,
    )

    with pytest.raises(UserAuthError):
        indexer.precheck()


