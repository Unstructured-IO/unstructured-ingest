import json
import os
import time
from pathlib import Path
from uuid import uuid4

import pytest
from _pytest.fixtures import TopRequest
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    ComplexField,
    CorsOptions,
    HnswAlgorithmConfiguration,
    HnswParameters,
    SearchField,
    SearchFieldDataType,
    SearchIndex,
    SimpleField,
    VectorSearch,
    VectorSearchAlgorithmMetric,
    VectorSearchProfile,
)

from test.integration.connectors.utils.constants import DESTINATION_TAG, VECTOR_DB_TAG
from test.integration.connectors.utils.validation.destination import (
    StagerValidationConfigs,
    stager_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.processes.connectors.azure_ai_search import (
    CONNECTOR_TYPE,
    RECORD_ID_LABEL,
    AzureAISearchAccessConfig,
    AzureAISearchConnectionConfig,
    AzureAISearchUploader,
    AzureAISearchUploaderConfig,
    AzureAISearchUploadStager,
    AzureAISearchUploadStagerConfig,
)

repo_path = Path(__file__).parent.resolve()

API_KEY = "AZURE_SEARCH_API_KEY"
ENDPOINT = "https://ingest-test-azure-cognitive-search.search.windows.net"


def get_api_key() -> str:
    key = os.environ[API_KEY]
    return key


def get_fields() -> list:
    data_source_fields = [
        SimpleField(name="url", type=SearchFieldDataType.String),
        SimpleField(name="version", type=SearchFieldDataType.String),
        SimpleField(name="date_created", type=SearchFieldDataType.DateTimeOffset),
        SimpleField(name="date_modified", type=SearchFieldDataType.DateTimeOffset),
        SimpleField(name="date_processed", type=SearchFieldDataType.DateTimeOffset),
        SimpleField(name="permissions_data", type=SearchFieldDataType.String),
        SimpleField(name="record_locator", type=SearchFieldDataType.String),
    ]
    coordinates_fields = [
        SimpleField(name="system", type=SearchFieldDataType.String),
        SimpleField(name="layout_width", type=SearchFieldDataType.Double),
        SimpleField(name="layout_height", type=SearchFieldDataType.Double),
        SimpleField(name="points", type=SearchFieldDataType.String),
    ]
    metadata_fields = [
        SimpleField(name="orig_elements", type=SearchFieldDataType.String),
        SimpleField(name="category_depth", type=SearchFieldDataType.Int32),
        SimpleField(name="parent_id", type=SearchFieldDataType.String),
        SimpleField(name="attached_to_filename", type=SearchFieldDataType.String),
        SimpleField(name="filetype", type=SearchFieldDataType.String),
        SimpleField(name="last_modified", type=SearchFieldDataType.DateTimeOffset),
        SimpleField(name="is_continuation", type=SearchFieldDataType.Boolean),
        SimpleField(name="file_directory", type=SearchFieldDataType.String),
        SimpleField(name="filename", type=SearchFieldDataType.String),
        ComplexField(name="data_source", fields=data_source_fields),
        ComplexField(name="coordinates", fields=coordinates_fields),
        SimpleField(
            name="languages", type=SearchFieldDataType.Collection(SearchFieldDataType.String)
        ),
        SimpleField(name="page_number", type=SearchFieldDataType.String),
        SimpleField(name="links", type=SearchFieldDataType.Collection(SearchFieldDataType.String)),
        SimpleField(name="page_name", type=SearchFieldDataType.String),
        SimpleField(name="url", type=SearchFieldDataType.String),
        SimpleField(
            name="link_urls", type=SearchFieldDataType.Collection(SearchFieldDataType.String)
        ),
        SimpleField(
            name="link_texts", type=SearchFieldDataType.Collection(SearchFieldDataType.String)
        ),
        SimpleField(
            name="sent_from", type=SearchFieldDataType.Collection(SearchFieldDataType.String)
        ),
        SimpleField(
            name="sent_to", type=SearchFieldDataType.Collection(SearchFieldDataType.String)
        ),
        SimpleField(name="subject", type=SearchFieldDataType.String),
        SimpleField(name="section", type=SearchFieldDataType.String),
        SimpleField(name="header_footer_type", type=SearchFieldDataType.String),
        SimpleField(
            name="emphasized_text_contents",
            type=SearchFieldDataType.Collection(SearchFieldDataType.String),
        ),
        SimpleField(
            name="emphasized_text_tags",
            type=SearchFieldDataType.Collection(SearchFieldDataType.String),
        ),
        SimpleField(name="text_as_html", type=SearchFieldDataType.String),
        SimpleField(name="regex_metadata", type=SearchFieldDataType.String),
        SimpleField(name="detection_class_prob", type=SearchFieldDataType.Double),
    ]
    fields = [
        SimpleField(name="id", type=SearchFieldDataType.String, key=True),
        SimpleField(name=RECORD_ID_LABEL, type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="element_id", type=SearchFieldDataType.String),
        SimpleField(name="text", type=SearchFieldDataType.String),
        SimpleField(name="type", type=SearchFieldDataType.String),
        ComplexField(name="metadata", fields=metadata_fields),
        SearchField(
            name="embeddings",
            type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
            vector_search_dimensions=384,
            vector_search_profile_name="embeddings-config-profile",
        ),
    ]
    return fields


def get_vector_search() -> VectorSearch:
    return VectorSearch(
        algorithms=[
            HnswAlgorithmConfiguration(
                name="hnsw-config",
                parameters=HnswParameters(
                    metric=VectorSearchAlgorithmMetric.COSINE,
                ),
            )
        ],
        profiles=[
            VectorSearchProfile(
                name="embeddings-config-profile", algorithm_configuration_name="hnsw-config"
            )
        ],
    )


def get_search_index_client() -> SearchIndexClient:
    api_key = get_api_key()
    return SearchIndexClient(ENDPOINT, AzureKeyCredential(api_key))


@pytest.fixture
def index() -> str:
    random_id = str(uuid4())[:8]
    index_name = f"utic-test-{random_id}"
    client = get_search_index_client()
    index = SearchIndex(
        name=index_name,
        fields=get_fields(),
        vector_search=get_vector_search(),
        cors_options=CorsOptions(allowed_origins=["*"], max_age_in_seconds=60),
    )
    print(f"creating index: {index_name}")
    client.create_index(index=index)
    try:
        yield index_name
    finally:
        print(f"deleting index: {index_name}")
        client.delete_index(index)


def validate_count(
    search_client: SearchClient, expected_count: int, retries: int = 10, interval: int = 1
) -> None:
    index_count = search_client.get_document_count()
    if index_count == expected_count:
        return
    tries = 0
    while tries < retries:
        time.sleep(interval)
        index_count = search_client.get_document_count()
        if index_count == expected_count:
            break
    assert index_count == expected_count, (
        f"Expected count ({expected_count}) doesn't match how "
        f"much came back from index: {index_count}"
    )


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
@requires_env("AZURE_SEARCH_API_KEY")
async def test_azure_ai_search_destination(
    upload_file: Path,
    index: str,
    tmp_path: Path,
):
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data",
    )
    stager = AzureAISearchUploadStager(upload_stager_config=AzureAISearchUploadStagerConfig())

    uploader = AzureAISearchUploader(
        connection_config=AzureAISearchConnectionConfig(
            access_config=AzureAISearchAccessConfig(key=get_api_key()),
            endpoint=ENDPOINT,
            index=index,
        ),
        upload_config=AzureAISearchUploaderConfig(),
    )
    staged_filepath = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file.name,
    )
    uploader.precheck()
    uploader.run(path=staged_filepath, file_data=file_data)

    # Run validation
    with staged_filepath.open() as f:
        staged_elements = json.load(f)
    expected_count = len(staged_elements)
    with uploader.connection_config.get_search_client() as search_client:
        validate_count(search_client=search_client, expected_count=expected_count)

    # Rerun and make sure the same documents get updated
    uploader.run(path=staged_filepath, file_data=file_data)
    with uploader.connection_config.get_search_client() as search_client:
        validate_count(search_client=search_client, expected_count=expected_count)


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
@requires_env("AZURE_SEARCH_API_KEY")
async def test_azure_ai_search_destination_drops_unknown_nested_fields(
    upload_file: Path,
    index: str,
    tmp_path: Path,
):
    """Regression test for error:
    "The property 'table_extraction_method' does not exist on type
    'search.complex.metadata' or is not present in the API version '2025-09-01'."

    The uploader must drop nested fields not declared in the index's complex schema
    (and stray top-level fields) before calling Azure, so the upload still succeeds.
    """
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data nested filter",
    )
    stager = AzureAISearchUploadStager(upload_stager_config=AzureAISearchUploadStagerConfig())

    uploader = AzureAISearchUploader(
        connection_config=AzureAISearchConnectionConfig(
            access_config=AzureAISearchAccessConfig(key=get_api_key()),
            endpoint=ENDPOINT,
            index=index,
        ),
        upload_config=AzureAISearchUploaderConfig(),
    )
    staged_filepath = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=tmp_path,
        output_filename=upload_file.name,
    )

    with staged_filepath.open() as f:
        staged_elements = json.load(f)
    assert staged_elements, "expected staged elements for the regression test"
    for element in staged_elements:
        element.setdefault("metadata", {})["table_extraction_method"] = "auto"
        element["unsupported_top_level_field"] = "drop me"

    uploader.precheck()
    uploader.run_data(data=staged_elements, file_data=file_data)

    expected_count = len(staged_elements)
    with uploader.connection_config.get_search_client() as search_client:
        validate_count(search_client=search_client, expected_count=expected_count)


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
@requires_env("AZURE_SEARCH_API_KEY")
async def test_azure_ai_search_destination_rejects_unknown_nested_fields_when_unfiltered(
    index: str,
):
    """Azure-side contract test: confirms the upstream cause of the production 400.

    Bypasses ``filter_doc`` by calling ``write_dict`` directly with a document that
    declares ``metadata.table_extraction_method`` — a field not present in the index's
    complex metadata schema. Azure must reject the upload, and the uploader must
    surface that error mentioning the offending property and complex type.

    If this test starts passing without raising, Azure has loosened its strict-typing
    rules and the recursive filter may no longer be necessary.
    """
    uploader = AzureAISearchUploader(
        connection_config=AzureAISearchConnectionConfig(
            access_config=AzureAISearchAccessConfig(key=get_api_key()),
            endpoint=ENDPOINT,
            index=index,
        ),
        upload_config=AzureAISearchUploaderConfig(),
    )

    bad_doc = {
        "id": "expect-rejection-1",
        RECORD_ID_LABEL: "regression-azure-strict-typing",
        "text": "should never be indexed",
        "metadata": {
            "filename": "doc.pdf",
            "filetype": "pdf",
            "table_extraction_method": "auto",
        },
    }

    with (
        uploader.connection_config.get_search_client() as search_client,
        pytest.raises(DestinationConnectionError) as excinfo,
    ):
        uploader.write_dict(elements_dict=[bad_doc], search_client=search_client)

    error_message = str(excinfo.value)
    assert "table_extraction_method" in error_message
    assert "search.complex.metadata" in error_message


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, VECTOR_DB_TAG)
@pytest.mark.parametrize("upload_file_str", ["upload_file_ndjson", "upload_file"])
def test_azure_ai_search_stager(
    request: TopRequest,
    upload_file_str: str,
    tmp_path: Path,
):
    upload_file: Path = request.getfixturevalue(upload_file_str)
    stager = AzureAISearchUploadStager()
    stager_validation(
        configs=StagerValidationConfigs(test_id=CONNECTOR_TYPE, expected_count=22),
        input_file=upload_file,
        stager=stager,
        tmp_dir=tmp_path,
    )
