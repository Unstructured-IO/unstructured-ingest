import json
import os
import time
from pathlib import Path
from typing import Generator
from uuid import uuid4

import pytest
import requests

from test.integration.connectors.utils.constants import DESTINATION_TAG, NOSQL_TAG
from test.integration.utils import requires_env
from unstructured_ingest.v2.interfaces.file_data import FileData, SourceIdentifiers
from unstructured_ingest.v2.logger import logger
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
def _get_jwt_token():
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

    response = requests.post(token_endpoint, headers=headers, data=data)
    response.raise_for_status()
    response_json = response.json()

    return response_json.get("access_token")


def query_data(corpus_key: str, element_id: str) -> dict:

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

    jwt_token = _get_jwt_token()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {jwt_token}",
        "X-source": "unstructured",
    }

    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    response_json = response.json()

    return response_json


def create_corpora(corpus_key: str, corpus_name: str) -> None:
    url = "https://api.vectara.io/v2/corpora"
    data = json.dumps({"key": corpus_key, "name": corpus_name, "description": "integration test"})
    jwt_token = _get_jwt_token()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {jwt_token}",
        "X-source": "unstructured",
    }

    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()


def replace_filter_attributes(corpus_key: str) -> None:
    url = f"https://api.vectara.io/v2/corpora/{corpus_key}/replace_filter_attributes"
    data = json.dumps(
        {
            "filter_attributes": [
                {"name": "element_id", "level": "part", "indexed": True, "type": "text"}
            ]
        }
    )
    jwt_token = _get_jwt_token()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {jwt_token}",
        "X-source": "unstructured",
    }

    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()


def delete_corpora(corpus_key: str) -> None:
    url = f"https://api.vectara.io/v2/corpora/{corpus_key}"

    jwt_token = _get_jwt_token()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {jwt_token}",
        "X-source": "unstructured",
    }

    response = requests.delete(url, headers=headers)
    response.raise_for_status()


def list_corpora() -> list:
    url = "https://api.vectara.io/v2/corpora?limit=100"
    jwt_token = _get_jwt_token()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {jwt_token}",
        "X-source": "unstructured",
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    response_json = response.json()
    if response_json.get("corpora"):
        return [item["key"] for item in response_json.get("corpora")]
    else:
        return []


def wait_for_ready(corpus_key: str, timeout=60, interval=2) -> None:
    def is_ready_status():
        corpora_list = list_corpora()
        return corpus_key in corpora_list

    start = time.time()
    is_ready = is_ready_status()
    while not is_ready and time.time() - start < timeout:
        time.sleep(interval)
        is_ready = is_ready_status()
    if not is_ready:
        raise TimeoutError("time out waiting for corpus to be ready")


def wait_for_delete(corpus_key: str, timeout=60, interval=2) -> None:
    start = time.time()
    while time.time() - start < timeout:
        corpora_list = list_corpora()
        if corpus_key not in corpora_list:
            return
        time.sleep(interval)

    raise TimeoutError("time out waiting for corpus to delete")


@pytest.fixture
def corpora_util() -> Generator[str, None, None]:
    random_id = str(uuid4()).split("-")[0]
    corpus_key = f"ingest-test-{random_id}"
    corpus_name = "ingest-test"
    logger.info(f"Creating corpus with key: {corpus_key}")
    try:
        create_corpora(corpus_key, corpus_name)
        replace_filter_attributes(corpus_key)
        wait_for_ready(corpus_key=corpus_key)
        yield corpus_key
    except Exception as e:
        logger.error(f"failed to create corpus {corpus_key}: {e}")
    finally:
        logger.info(f"deleting corpus: {corpus_key}")
        delete_corpora(corpus_key)
        wait_for_delete(corpus_key=corpus_key)


@pytest.mark.asyncio
@pytest.mark.tags(VECTARA_CONNECTOR_TYPE, DESTINATION_TAG, "vectara", NOSQL_TAG)
@requires_env("VECTARA_OAUTH_CLIENT_ID", "VECTARA_OAUTH_SECRET", "VECTARA_CUSTOMER_ID")
async def test_vectara_destination(
    upload_file: Path, tmp_path: Path, corpora_util: str, retries=30, interval=10
):
    corpus_key = corpora_util
    connection_kwargs = {
        "customer_id": os.environ["VECTARA_CUSTOMER_ID"],
        "corpus_key": corpus_key,
    }

    oauth_client_id = os.environ["VECTARA_OAUTH_CLIENT_ID"]
    oauth_secret = os.environ["VECTARA_OAUTH_SECRET"]

    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=VECTARA_CONNECTOR_TYPE,
        identifier="mock-file-data",
    )

    stager_config = VectaraUploadStagerConfig(batch_size=10)
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
        upload_config=VectaraUploaderConfig(),
    )

    with new_upload_file.open() as new_upload_fp:
        elements_stager = json.load(new_upload_fp)

    if uploader.is_async():
        await uploader.run_data_async(data=elements_stager, file_data=file_data)

    with upload_file.open() as upload_fp:
        elements = json.load(upload_fp)
    first_element = elements[0]

    for i in range(retries):
        response = query_data(corpus_key, first_element["element_id"])
        if not response["search_results"]:
            time.sleep(interval)
        else:
            break

    validate_upload(response=response, expected_data=first_element)
