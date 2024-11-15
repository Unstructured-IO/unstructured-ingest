import json
import os
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import pytest
from astrapy import Collection
from astrapy import DataAPIClient as AstraDBClient

from test.integration.connectors.utils.constants import (
    DESTINATION_TAG,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.astradb import (
    CONNECTOR_TYPE,
    AstraDBAccessConfig,
    AstraDBConnectionConfig,
    AstraDBUploader,
    AstraDBUploaderConfig,
    AstraDBUploadStager,
)


@dataclass(frozen=True)
class EnvData:
    api_endpoint: str
    token: str


def get_env_data() -> EnvData:
    return EnvData(
        api_endpoint=os.environ["ASTRA_DB_API_ENDPOINT"],
        token=os.environ["ASTRA_DB_APPLICATION_TOKEN"],
    )


@pytest.fixture
def collection(upload_file: Path) -> Collection:
    random_id = str(uuid4())[:8]
    collection_name = f"utic_test_{random_id}"
    with upload_file.open("r") as upload_fp:
        upload_data = json.load(upload_fp)
    first_content = upload_data[0]
    embeddings = first_content["embeddings"]
    embedding_dimension = len(embeddings)
    my_client = AstraDBClient()
    env_data = get_env_data()
    astra_db = my_client.get_database(
        api_endpoint=env_data.api_endpoint,
        token=env_data.token,
    )
    collection = astra_db.create_collection(collection_name, dimension=embedding_dimension)
    try:
        yield collection
    finally:
        astra_db.drop_collection(collection)


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG)
@requires_env("ASTRA_DB_API_ENDPOINT", "ASTRA_DB_APPLICATION_TOKEN")
async def test_azure_ai_search_destination(
    upload_file: Path,
    collection: Collection,
    tmp_path: Path,
):
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock file data",
    )
    stager = AstraDBUploadStager()
    env_data = get_env_data()
    uploader = AstraDBUploader(
        connection_config=AstraDBConnectionConfig(
            access_config=AstraDBAccessConfig(
                api_endpoint=env_data.api_endpoint, token=env_data.token
            ),
        ),
        upload_config=AstraDBUploaderConfig(collection_name=collection.name),
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
    current_count = collection.count_documents(filter={}, upper_bound=expected_count * 2)
    assert current_count == expected_count, (
        f"Expected count ({expected_count}) doesn't match how "
        f"much came back from collection: {current_count}"
    )

    # Rerun and make sure the same documents get updated
    uploader.run(path=staged_filepath, file_data=file_data)
    current_count = collection.count_documents(filter={}, upper_bound=expected_count * 2)
    assert current_count == expected_count, (
        f"Expected count ({expected_count}) doesn't match how "
        f"much came back from collection: {current_count}"
    )
