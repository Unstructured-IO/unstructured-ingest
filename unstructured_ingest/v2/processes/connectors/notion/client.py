from typing import Any, Generator

import httpx
from notion_client import Client as NotionClient
from notion_client.api_endpoints import BlocksChildrenEndpoint as NotionBlocksChildrenEndpoint
from notion_client.api_endpoints import DatabasesEndpoint as NotionDatabasesEndpoint
from notion_client.errors import HTTPResponseError, RequestTimeoutError

from unstructured_ingest.v2.processes.connectors.notion.types.block import Block
from unstructured_ingest.v2.processes.connectors.notion.types.database import Database
from unstructured_ingest.v2.processes.connectors.notion.types.database_properties import map_cells
from unstructured_ingest.v2.processes.connectors.notion.types.page import Page


class AsyncBlocksChildrenEndpoint(NotionBlocksChildrenEndpoint):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._http_client = httpx.AsyncClient()

    async def list(self, block_id: str, **kwargs: Any) -> tuple[list[Block], dict]:
        """Fetch the list of child blocks asynchronously."""
        try:
            response = await self._http_client.get(
                f"{self.parent._api_base}/blocks/{block_id}/children", **kwargs
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise HTTPResponseError(f"Failed to list blocks: {str(e)}")
        except httpx.TimeoutException:
            raise RequestTimeoutError()

        resp = response.json()
        child_blocks = [Block.from_dict(data=b) for b in resp.pop("results", [])]
        return child_blocks, resp

    async def iterate_list(
        self, block_id: str, **kwargs: Any
    ) -> Generator[list[Block], None, None]:
        """Fetch the list of child blocks in pages asynchronously."""
        next_cursor = None
        while True:
            params = {"start_cursor": next_cursor} if next_cursor else {}
            params.update(kwargs)
            child_blocks, response = await self.list(block_id, **params)
            yield child_blocks

            next_cursor = response.get("next_cursor")
            if not response.get("has_more") or not next_cursor:
                return

    async def close(self):
        """Close the HTTP client."""
        await self._http_client.aclose()


class AsyncDatabasesEndpoint(NotionDatabasesEndpoint):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._http_client = httpx.AsyncClient()

    async def retrieve(self, database_id: str, **kwargs: Any) -> Database:
        """Fetch a database by its ID asynchronously."""
        try:
            response = await self._http_client.get(
                f"{self.parent._api_base}/databases/{database_id}", **kwargs
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise HTTPResponseError(f"Failed to retrieve database: {str(e)}")
        except httpx.TimeoutException:
            raise RequestTimeoutError()

        return Database.from_dict(data=response.json())

    async def query(self, database_id: str, **kwargs: Any) -> tuple[list[Page], dict]:
        """Query a database asynchronously."""
        try:
            response = await self._http_client.post(
                f"{self.parent._api_base}/databases/{database_id}/query",
                json=kwargs.get("json", {}),
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise HTTPResponseError(f"Failed to query database: {str(e)}")
        except httpx.TimeoutException:
            raise RequestTimeoutError()

        resp = response.json()
        pages = [Page.from_dict(data=p) for p in resp.pop("results", [])]
        for p in pages:
            p.properties = map_cells(p.properties)
        return pages, resp

    async def close(self):
        """Close the HTTP client."""
        await self._http_client.aclose()


class AsyncClient(NotionClient):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.blocks = AsyncBlocksChildrenEndpoint(parent=self)
        self.databases = AsyncDatabasesEndpoint(parent=self)

    async def close(self):
        """Close all async endpoints."""
        await self.blocks.close()
        await self.databases.close()
