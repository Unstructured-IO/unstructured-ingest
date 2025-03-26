import json
import os
import time
from functools import lru_cache
from pathlib import Path
from typing import Generator
from uuid import uuid4

import pytest
import requests

from test.integration.connectors.utils.constants import DESTINATION_TAG, NOSQL_TAG
from test.integration.utils import requires_env
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
from unstructured_ingest.v2.types.file_data import FileData, SourceIdentifiers


def validate_upload(document: dict, expected_data: dict):
    logger.info(f"validating document: {document}")
    element_id = expected_data["element_id"]
    expected_text = expected_data["text"]
    filename = expected_data["metadata"]["filename"]
    filetype = expected_data["metadata"]["filetype"]
    page_number = expected_data["metadata"]["page_number"]

    assert document is not None
    speech_parts = document["parts"]
    assert speech_parts
    first_part = speech_parts[0]
    assert first_part["text"] == expected_text
    part_metadata = first_part["metadata"]
    assert part_metadata
    assert part_metadata["element_id"] == element_id
    assert part_metadata["filename"] == filename
    assert part_metadata["filetype"] == filetype
    assert part_metadata["page_number"] == page_number


@requires_env("VECTARA_OAUTH_CLIENT_ID", "VECTARA_OAUTH_SECRET", "VECTARA_CUSTOMER_ID")
@lru_cache()
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


def list_documents(corpus_key: str) -> list[str]:

    url = f"https://api.vectara.io/v2/corpora/{corpus_key}/documents"

    # the query below requires the corpus to have filter attributes for element_id

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
    documents = response_json.get("documents", [])
    return documents


def fetch_document(corpus_key: str, documents_id: str) -> dict:
    url = f"https://api.vectara.io/v2/corpora/{corpus_key}/documents/{documents_id}"
    jwt_token = _get_jwt_token()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {jwt_token}",
        "X-source": "unstructured",
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


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


def get_metadata(corpus_key: str):
    url = f"https://api.vectara.io/v2/corpora/{corpus_key}"
    jwt_token = _get_jwt_token()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {jwt_token}",
        "X-source": "unstructured",
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def wait_for_ready(corpus_key: str, timeout=60, interval=2) -> None:
    start = time.time()
    while time.time() - start < timeout:
        try:
            get_metadata(corpus_key)
            return
        except requests.HTTPError:
            time.sleep(interval)
    raise TimeoutError("time out waiting for corpus to be ready")


def wait_for_delete(corpus_key: str, timeout=60, interval=2) -> None:
    start = time.time()
    while time.time() - start < timeout:
        try:
            get_metadata(corpus_key)
            time.sleep(interval)
        except requests.HTTPError:
            return
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


def wait_for_doc_meta(corpus_key: str, timeout=60, interval=1) -> list[str]:
    start = time.time()
    while time.time() - start < timeout:
        all_document_meta = list_documents(corpus_key)
        if not all_document_meta:
            time.sleep(interval)
            continue
        else:
            return all_document_meta
    raise TimeoutError("time out waiting for document to be ready")


@pytest.mark.asyncio
@pytest.mark.tags(VECTARA_CONNECTOR_TYPE, DESTINATION_TAG, "vectara", NOSQL_TAG)
@requires_env("VECTARA_OAUTH_CLIENT_ID", "VECTARA_OAUTH_SECRET", "VECTARA_CUSTOMER_ID")
async def test_vectara_destination(
    upload_file: Path, tmp_path: Path, corpora_util: str, retries=30, interval=1
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
        upload_config=VectaraUploaderConfig(),
    )

    with new_upload_file.open() as new_upload_fp:
        elements_stager = json.load(new_upload_fp)

    if uploader.is_async():
        await uploader.run_data_async(data=elements_stager, file_data=file_data)

    with upload_file.open() as upload_fp:
        elements = json.load(upload_fp)
    first_element = elements[0]

    all_document_meta = wait_for_doc_meta(corpus_key)
    assert len(all_document_meta) == 1
    document_meta = all_document_meta[0]
    document = fetch_document(corpus_key=corpus_key, documents_id=document_meta["id"])
    validate_upload(document=document, expected_data=first_element)
