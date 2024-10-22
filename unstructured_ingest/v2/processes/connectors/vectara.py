import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from dateutil import parser
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

BASE_URL = "https://api.vectara.io/v1"

CONNECTOR_TYPE = "vectara"


class VectaraAccessConfig(AccessConfig):
    oauth_client_id: Optional[str] = Field(default=None, sensitive=True, description="Client ID")
    oauth_secret: Optional[str] = Field(default=None, sensitive=True, description="Client Secret")


class VectaraConnectionConfig(ConnectionConfig):
    access_config: Secret[VectaraAccessConfig]
    customer_id: str
    corpus_name: Optional[str] = None
    corpus_id: Optional[str] = None
    token_url: str = "https://vectara-prod-{}.auth.us-west-2.amazoncognito.com/oauth2/token"


class VectaraUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class VectaraUploadStager(UploadStager):
    upload_stager_config: VectaraUploadStagerConfig = field(
        default_factory=lambda: VectaraUploadStagerConfig()
    )

    @staticmethod
    def parse_date_string(date_string: str) -> date:
        try:
            timestamp = float(date_string)
            return datetime.fromtimestamp(timestamp)
        except Exception as e:
            logger.debug(f"date {date_string} string not a timestamp: {e}")
        return parser.parse(date_string)

    @staticmethod
    def conform_dict(data: dict) -> dict:
        """
        Prepares dictionary in the format that Vectara requires

        Select which meta-data fields to include and optionally map them to a new new.
        remove the "metadata-" prefix from the keys
        """
        metadata_map = {
            "page_number": "page_number",
            "data_source-url": "url",
            "filename": "filename",
            "filetype": "filetype",
            "last_modified": "last_modified",
        }
        md = flatten_dict(data, separator="-", flatten_lists=True)
        md = {k.replace("metadata-", ""): v for k, v in md.items()}
        md = {metadata_map[k]: v for k, v in md.items() if k in metadata_map}
        return md

    def run(
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        docs_list: Dict[Dict[str, Any]] = []
        with open(elements_filepath) as elements_file:
            elements_contents = json.load(elements_file)

        docs_list: Dict[Dict[str, Any]] = []
        conformed_elements = {
            "documentId": str(uuid.uuid4()),
            "title": elements_contents[0]
            .get("metadata", {})
            .get("data_source", {})
            .get("record_locator", {})
            .get("path"),
            "section": [
                {
                    "text": element.pop("text", None),
                    "metadataJson": json.dumps(self.conform_dict(data=element)),
                }
                for element in elements_contents
            ],
        }
        logger.info(
            f"Extending {len(conformed_elements)} json elements from content in {elements_filepath}"
        )
        docs_list.append(conformed_elements)

        output_path = Path(output_dir) / Path(f"{output_filename}.json")
        with open(output_path, "w") as output_file:
            json.dump(docs_list, output_file)
        return output_path


class VectaraUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=50, description="Number of records per batch")


@dataclass
class VectaraUploader(Uploader):

    connector_type: str = CONNECTOR_TYPE
    upload_config: VectaraUploaderConfig
    connection_config: VectaraConnectionConfig
    _jwt_token: Optional[str] = field(init=False, default=None)
    _jwt_token_expires_ts: Optional[float] = field(init=False, default=None)

    def is_async(self) -> bool:
        return True

    async def precheck(self) -> None:
        try:
            self.vectara()
        except Exception as e:
            logger.error(f"Failed to validate connection {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @property
    async def jwt_token(self):
        if not self._jwt_token or self._jwt_token_expires_ts - datetime.now().timestamp() <= 60:
            self._jwt_token = self._get_jwt_token()
        return self._jwt_token

    # Get Oauth2 JWT token
    @requires_dependencies(["httpx"], extras="vectara")
    async def _get_jwt_token(self):
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

    @DestinationConnectionError.wrap
    async def vectara(self):
        """
        Check the connection for Vectara and validate corpus exists.
        - If more than one corpus with the same name exists - then return a message
        - If exactly one corpus exists with this name - use it.
        - If does not exist - create it.
        """
        # Get token if not already set
        await self.jwt_token

        list_corpora_response = await self._request(
            endpoint="list-corpora",
            data={"numResults": 1, "filter": self.connection_config.corpus_name},
        )

        possible_corpora_ids_names_map = {
            corpus.get("id"): corpus.get("name")
            for corpus in list_corpora_response.get("corpus")
            if corpus.get("name") == self.connection_config.corpus_name
        }

        if len(possible_corpora_ids_names_map) > 1:
            return f"Multiple Corpora exist with name {self.connection_config.corpus_name}"
        if len(possible_corpora_ids_names_map) == 1:
            self.connection_config.corpus_id = list(possible_corpora_ids_names_map.keys())[0]
        else:
            data = {
                "corpus": {
                    "name": self.connection_config.corpus_name,
                }
            }
            create_corpus_response = await self._request(endpoint="create-corpus", data=data)
            self.connection_config.corpus_id = create_corpus_response.get("corpusId")

    @requires_dependencies(["httpx"], extras="vectara")
    async def _request(
        self,
        endpoint: str,
        http_method: str = "POST",
        params: Mapping[str, Any] = None,
        data: Mapping[str, Any] = None,
    ):
        import httpx

        url = f"{BASE_URL}/{endpoint}"

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {await self.jwt_token}",
            "customer-id": self.connection_config.customer_id,
            "X-source": "unstructured",
        }

        async with httpx.AsyncClient() as client:
            response = await client.request(
                method=http_method, url=url, headers=headers, params=params, json=data
            )
            response.raise_for_status()
            return response.json()

    async def _delete_doc(self, doc_id: str) -> None:
        """
        Delete a document from the Vectara corpus.

        Args:
            url (str): URL of the page to delete.
            doc_id (str): ID of the document to delete.
        """
        body = {
            "customer_id": self.connection_config.customer_id,
            "corpus_id": self.connection_config.corpus_id,
            "document_id": doc_id,
        }
        await self._request(endpoint="delete-doc", data=body)

    async def _index_document(self, document: Dict[str, Any]) -> None:
        """
        Index a document (by uploading it to the Vectara corpus) from the document dictionary
        """
        body = {
            "customer_id": self.connection_config.customer_id,
            "corpus_id": self.connection_config.corpus_id,
            "document": document,
        }

        try:
            result = await self._request(endpoint="index", data=body, http_method="POST")
        except Exception as e:
            logger.info(f"exception {e} while indexing document {document['documentId']}")
            return

        if (
            "status" in result
            and result["status"]
            and (
                "ALREADY_EXISTS" in result["status"]["code"]
                or (
                    "CONFLICT" in result["status"]["code"]
                    and "Indexing doesn't support updating documents"
                    in result["status"]["statusDetail"]
                )
            )
        ):
            logger.info(f"document {document['documentId']} already exists, re-indexing")
            await self._delete_doc(document["documentId"])
            result = await self._request(endpoint="index", data=body, http_method="POST")
            return

        if "status" in result and result["status"] and "OK" in result["status"]["code"]:
            logger.info(f"indexing document {document['documentId']} succeeded")
        else:
            logger.info(f"indexing document {document['documentId']} failed, response = {result}")

    async def write_dict(self, *args, docs_list: List[Dict[str, Any]], **kwargs) -> None:
        logger.info(f"inserting / updating {len(docs_list)} documents to Vectara ")
        await asyncio.gather(*(self._index_document(vdoc) for vdoc in docs_list))

    async def run_async(
        self,
        path: Path,
        file_data: FileData,
        **kwargs: Any,
    ) -> Path:
        import aiofiles

        docs_list: Dict[Dict[str, Any]] = []

        async with aiofiles.open(path) as json_file:
            docs_list = json.loads(await json_file.read())
        await self.write_dict(docs_list=docs_list)


vectara_destination_entry = DestinationRegistryEntry(
    connection_config=VectaraConnectionConfig,
    uploader=VectaraUploader,
    uploader_config=VectaraUploaderConfig,
    upload_stager=VectaraUploadStager,
    upload_stager_config=VectaraUploadStagerConfig,
)
