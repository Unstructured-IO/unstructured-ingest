import json
import os
from pathlib import Path

import pytest

from test.integration.connectors.utils.constants import DESTINATION_TAG
from test.integration.utils import requires_env
from unstructured_ingest.v2.interfaces.file_data import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.vectara import (
    CONNECTOR_TYPE as VECTARA_CONNECTOR_TYPE,
)
from unstructured_ingest.v2.processes.connectors.vectara import (
    VectaraAccessConfig,
    VectaraConnectionConfig,
    VectaraUploader,
    VectaraUploaderConfig,
    VectaraUploadStager,
    VectaraUploadStagerConfig,
)


def validate_upload(response: dict, expected_data: dict):
    element_id = expected_data["element_id"]
    expected_text = expected_data["text"]
    filename = expected_data["metadata"]["filename"]
    filetype = expected_data["metadata"]["filetype"]
    page_number = expected_data["metadata"]["page_number"]

    response = response["search_results"][0]

    assert response is not None
    assert response["text"] == expected_text
    assert response["part_metadata"]["element_id"] == element_id
    assert response["part_metadata"]["filename"] == filename
    assert response["part_metadata"]["filetype"] == filetype
    assert response["part_metadata"]["page_number"] == page_number


@requires_env("VECTARA_OAUTH_CLIENT_ID", "VECTARA_OAUTH_SECRET", "VECTARA_CUSTOMER_ID")
async def _get_jwt_token():
    import httpx

    """Connect to the server and get a JWT token."""
    customer_id = os.environ["VECTARA_CUSTOMER_ID"]
    token_endpoint = (
        f"https://vectara-prod-{customer_id}.auth.us-west-2.amazoncognito.com/oauth2/token"
    )
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": os.environ["VECTARA_OAUTH_CLIENT_ID"],
        "client_secret": os.environ["VECTARA_OAUTH_SECRET"],
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(token_endpoint, headers=headers, data=data)
        response.raise_for_status()
        response_json = response.json()

    return response_json.get("access_token")


async def query_data(corpus_key: str, element_id: str):
    import httpx

    url = f"https://api.vectara.io/v2/corpora/{corpus_key}/query"

    # the query below requires the corpus to have filter attributes for element_id
    data = json.dumps(
        {
            "query": "string",
            "search": {
                "metadata_filter": f"part.element_id = '{element_id}'",
                "lexical_interpolation": 1,
                "limit": 10,
            },
        }
    )
    jwt_token = await _get_jwt_token()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {jwt_token}",
        "X-source": "unstructured",
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(url, headers=headers, data=data)
        response.raise_for_status()
        response_json = response.json()

    return response_json


@pytest.mark.asyncio
@pytest.mark.tags(VECTARA_CONNECTOR_TYPE, DESTINATION_TAG, "vectara")
@requires_env("VECTARA_OAUTH_CLIENT_ID", "VECTARA_OAUTH_SECRET", "VECTARA_CUSTOMER_ID")
async def test_vectara_destination(upload_file: Path, tmp_path: Path):
    connection_kwargs = {
        "customer_id": os.environ["VECTARA_CUSTOMER_ID"],
        "corpus_name": "test-corpus-vectara-24162",
        "corpus_key": "test-corpus-vectara-24162_3535",
    }
    oauth_client_id = os.environ["VECTARA_OAUTH_CLIENT_ID"]
    oauth_secret = os.environ["VECTARA_OAUTH_SECRET"]

    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=VECTARA_CONNECTOR_TYPE,
        identifier="mock-file-data",
    )

    stager_config = VectaraUploadStagerConfig()
    stager = VectaraUploadStager(upload_stager_config=stager_config)
    new_upload_file = stager.run(
        elements_filepath=upload_file,
        output_dir=tmp_path,
        output_filename=upload_file.name,
        file_data=file_data,
    )

    uploader = VectaraUploader(
        connection_config=VectaraConnectionConfig(
            **connection_kwargs,
            access_config=VectaraAccessConfig(
                oauth_client_id=oauth_client_id, oauth_secret=oauth_secret
            ),
        ),
        upload_config=VectaraUploaderConfig(batch_size=10),
    )

    if uploader.is_async():
        await uploader.run_async(path=new_upload_file, file_data=file_data)

    with upload_file.open() as upload_fp:
        elements = json.load(upload_fp)
    first_element = elements[0]

    response = await query_data(connection_kwargs["corpus_key"], first_element["element_id"])

    validate_upload(response=response, expected_data=first_element)
