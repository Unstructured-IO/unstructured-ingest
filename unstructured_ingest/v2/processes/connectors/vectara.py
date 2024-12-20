import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.data_prep import flatten_dict
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    FileData,
    Uploader,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry

BASE_URL = "https://api.vectara.io/v2"

CONNECTOR_TYPE = "vectara"


class VectaraAccessConfig(AccessConfig):
    oauth_client_id: str = Field(description="Client ID")
    oauth_secret: str = Field(description="Client Secret")


class VectaraConnectionConfig(ConnectionConfig):
    access_config: Secret[VectaraAccessConfig]
    customer_id: str
    corpus_name: Optional[str] = None
    corpus_key: Optional[str] = None
    token_url: str = "https://vectara-prod-{}.auth.us-west-2.amazoncognito.com/oauth2/token"


class VectaraUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class VectaraUploadStager(UploadStager):
    upload_stager_config: VectaraUploadStagerConfig = field(
        default_factory=lambda: VectaraUploadStagerConfig()
    )

    @staticmethod
    def conform_dict(data: dict) -> dict:
        """
        Prepares dictionary in the format that Vectara requires.
        See more detail in https://docs.vectara.com/docs/rest-api/create-corpus-document

        Select which meta-data fields to include and optionally map them to a new format.
        remove the "metadata-" prefix from the keys
        """
        metadata_map = {
            "page_number": "page_number",
            "data_source-url": "url",
            "filename": "filename",
            "filetype": "filetype",
            "last_modified": "last_modified",
            "element_id": "element_id",
        }
        md = flatten_dict(data, separator="-", flatten_lists=True)
        md = {k.replace("metadata-", ""): v for k, v in md.items()}
        md = {metadata_map[k]: v for k, v in md.items() if k in metadata_map}
        return md

    def process_whole(self, input_file: Path, output_file: Path, file_data: FileData) -> None:
        with input_file.open() as in_f:
            elements_contents = json.load(in_f)

        logger.info(
            f"Extending {len(elements_contents)} json elements from content in {input_file}"
        )

        conformed_elements = [
            {
                "id": str(uuid.uuid4()),
                "type": "core",
                "metadata": {
                    "title": file_data.identifier,
                },
                "document_parts": [
                    {
                        "text": element.pop("text", None),
                        "metadata": self.conform_dict(data=element),
                    }
                    for element in elements_contents
                ],
            }
        ]

        with open(output_file, "w") as out_f:
            json.dump(conformed_elements, out_f, indent=2)


class VectaraUploaderConfig(UploaderConfig):
    pass


@dataclass
class VectaraUploader(Uploader):

    connector_type: str = CONNECTOR_TYPE
    upload_config: VectaraUploaderConfig
    connection_config: VectaraConnectionConfig
    _jwt_token: Optional[str] = field(init=False, default=None)
    _jwt_token_expires_ts: Optional[float] = field(init=False, default=None)

    def is_async(self) -> bool:
        return True

    def precheck(self) -> None:
        try:
            self._check_connection_and_corpora()
        except Exception as e:
            logger.error(f"Failed to validate connection {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @property
    async def jwt_token_async(self) -> str:
        if not self._jwt_token or self._jwt_token_expires_ts - datetime.now().timestamp() <= 60:
            self._jwt_token = await self._get_jwt_token_async()
        return self._jwt_token

    @property
    def jwt_token(self) -> str:
        if not self._jwt_token or self._jwt_token_expires_ts - datetime.now().timestamp() <= 60:
            self._jwt_token = self._get_jwt_token()
        return self._jwt_token

    # Get Oauth2 JWT token
    @requires_dependencies(["httpx"], extras="vectara")
    async def _get_jwt_token_async(self) -> str:
        import httpx

        """Connect to the server and get a JWT token."""
        token_endpoint = self.connection_config.token_url.format(self.connection_config.customer_id)
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {
            "grant_type": "client_credentials",
            "client_id": self.connection_config.access_config.get_secret_value().oauth_client_id,
            "client_secret": self.connection_config.access_config.get_secret_value().oauth_secret,
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(token_endpoint, headers=headers, data=data)
            response.raise_for_status()
            response_json = response.json()

        request_time = datetime.now().timestamp()
        self._jwt_token_expires_ts = request_time + response_json.get("expires_in")

        return response_json.get("access_token")

    # Get Oauth2 JWT token
    @requires_dependencies(["httpx"], extras="vectara")
    def _get_jwt_token(self) -> str:
        import httpx

        """Connect to the server and get a JWT token."""
        token_endpoint = self.connection_config.token_url.format(self.connection_config.customer_id)
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {
            "grant_type": "client_credentials",
            "client_id": self.connection_config.access_config.get_secret_value().oauth_client_id,
            "client_secret": self.connection_config.access_config.get_secret_value().oauth_secret,
        }

        with httpx.Client() as client:
            response = client.post(token_endpoint, headers=headers, data=data)
            response.raise_for_status()
            response_json = response.json()

        request_time = datetime.now().timestamp()
        self._jwt_token_expires_ts = request_time + response_json.get("expires_in")

        return response_json.get("access_token")

    @DestinationConnectionError.wrap
    def _check_connection_and_corpora(self) -> None:
        """
        Check the connection for Vectara and validate corpus exists.
        - If more than one corpus with the same name exists - raise error
        - If exactly one corpus exists with this name - use it.
        - If does not exist - raise error.
        """
        # Get token if not already set
        self.jwt_token

        _, list_corpora_response = self._request(
            http_method="GET",
            endpoint="corpora",
        )

        if self.connection_config.corpus_name:
            possible_corpora_keys_names_map = {
                corpus.get("key"): corpus.get("name")
                for corpus in list_corpora_response.get("corpora")
                if corpus.get("name") == self.connection_config.corpus_name
            }

            if len(possible_corpora_keys_names_map) > 1:
                raise ValueError(
                    f"Multiple Corpus exist with name {self.connection_config.corpus_name} in dest."
                )
            if len(possible_corpora_keys_names_map) == 1:
                if not self.connection_config.corpus_key:
                    self.connection_config.corpus_key = list(
                        possible_corpora_keys_names_map.keys()
                    )[0]
                elif (
                    self.connection_config.corpus_key
                    != list(possible_corpora_keys_names_map.keys())[0]
                ):
                    raise ValueError("Corpus key does not match provided corpus name.")
            else:
                raise ValueError(
                    f"No Corpora exist with name {self.connection_config.corpus_name} in dest."
                )

    @requires_dependencies(["httpx"], extras="vectara")
    async def _async_request(
        self,
        endpoint: str,
        http_method: str = "POST",
        params: Mapping[str, Any] = None,
        data: Mapping[str, Any] = None,
    ) -> tuple[bool, dict]:
        import httpx

        url = f"{BASE_URL}/{endpoint}"

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {await self.jwt_token_async}",
            "X-source": "unstructured",
        }

        async with httpx.AsyncClient() as client:
            response = await client.request(
                method=http_method, url=url, headers=headers, params=params, json=data
            )
            response.raise_for_status()
            return response.json()

    @requires_dependencies(["httpx"], extras="vectara")
    def _request(
        self,
        endpoint: str,
        http_method: str = "POST",
        params: Mapping[str, Any] = None,
        data: Mapping[str, Any] = None,
    ) -> tuple[bool, dict]:
        import httpx

        url = f"{BASE_URL}/{endpoint}"

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.jwt_token}",
            "X-source": "unstructured",
        }

        with httpx.Client() as client:
            response = client.request(
                method=http_method, url=url, headers=headers, params=params, json=data
            )
            response.raise_for_status()
            return response.json()

    async def _delete_doc(self, doc_id: str) -> tuple[bool, dict]:
        """
        Delete a document from the Vectara corpus.
        """

        return await self._async_request(
            endpoint=f"corpora/{self.connection_config.corpus_key}/documents/{doc_id}",
            http_method="DELETE",
        )

    async def _index_document(self, document: Dict[str, Any]) -> None:
        """
        Index a document (by uploading it to the Vectara corpus) from the document dictionary
        """

        logger.debug(
            f"Indexing document {document['id']} to corpus key {self.connection_config.corpus_key}"
        )

        try:
            result = await self._async_request(
                endpoint=f"corpora/{self.connection_config.corpus_key}/documents", data=document
            )
        except Exception as e:
            logger.error(f"exception {e} while indexing document {document['id']}")
            return

        if (
            "messages" in result
            and result["messages"]
            and (
                "ALREADY_EXISTS" in result["messages"]
                or (
                    "CONFLICT: Indexing doesn't support updating documents."
                    in result["messages"][0]
                )
            )
        ):
            logger.info(f"document {document['id']} already exists, re-indexing")
            await self._delete_doc(document["id"])
            await self._async_request(
                endpoint=f"corpora/{self.connection_config.corpus_key}/documents", data=document
            )
            return

        logger.info(f"indexing document {document['id']} succeeded")

    async def run_data_async(
        self,
        data: list[dict],
        file_data: FileData,
        **kwargs: Any,
    ) -> None:

        logger.info(f"inserting / updating {len(data)} documents to Vectara ")
        await asyncio.gather(*(self._index_document(vdoc) for vdoc in data))


vectara_destination_entry = DestinationRegistryEntry(
    connection_config=VectaraConnectionConfig,
    uploader=VectaraUploader,
    uploader_config=VectaraUploaderConfig,
    upload_stager=VectaraUploadStager,
    upload_stager_config=VectaraUploadStagerConfig,
)
