# servicenow.py

from dataclasses import dataclass, field
from typing import Generator, Optional, Any, List
from pathlib import Path
import requests

from pydantic import Field, Secret

from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Indexer,
    IndexerConfig,
    Downloader,
    DownloaderConfig,
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
    DownloadResponse,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import SourceRegistryEntry

CONNECTOR_TYPE = "servicenow"


class ServiceNowAccessConfig(AccessConfig):
    token: str = Field(description="Personal Access Token for ServiceNow")


class ServiceNowConnectionConfig(ConnectionConfig):
    instance_url: str = Field(description="Base URL of ServiceNow instance, e.g., https://dev12345.service-now.com")
    access_config: Secret[ServiceNowAccessConfig]

    def get_auth_headers(self) -> dict:
        access = self.access_config.get_secret_value()
        return {
            "Authorization": f"Bearer {access.token}",
            "Accept": "application/json",
        }


class ServiceNowIndexerConfig(IndexerConfig):
    tables: List[str] = Field(default_factory=lambda: ["kb_knowledge", "incident"], description="ServiceNow tables to fetch")
    limit: int = Field(default=50, description="Maximum number of records to fetch per table")


@dataclass
class ServiceNowIndexer(Indexer):
    connection_config: ServiceNowConnectionConfig
    index_config: ServiceNowIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def run(self, **kwargs) -> Generator[FileData, None, None]:
        headers = self.connection_config.get_auth_headers()

        for table in self.index_config.tables:
            url = f"{self.connection_config.instance_url}/api/now/table/{table}?sysparm_limit={self.index_config.limit}"
            logger.info(f"[{self.connector_type}] Fetching from {table} via {url}")

            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
            except Exception as e:
                logger.error(f"Failed to fetch data from {table}: {e}", exc_info=True)
                continue

            records = response.json().get("result", [])

            for record in records:
                sys_id = record.get("sys_id")
                filename = f"{sys_id}.txt"
                rel_path = f"{table}/{filename}"

                yield FileData(
                    identifier=sys_id,
                    connector_type=self.connector_type,
                    metadata=FileDataSourceMetadata(
                        date_processed=record.get("sys_updated_on"),
                        url=f"{self.connection_config.instance_url}/nav_to.do?uri={table}.do?sys_id={sys_id}",
                        record_locator={"table": table, "sys_id": sys_id},
                    ),
                    additional_metadata=record,
                    source_identifiers=SourceIdentifiers(
                        filename=filename,
                        rel_path=rel_path,
                        fullpath=rel_path,
                    ),
                )


class ServiceNowDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class ServiceNowDownloader(Downloader):
    connection_config: ServiceNowConnectionConfig
    download_config: ServiceNowDownloaderConfig = field(default_factory=ServiceNowDownloaderConfig)
    connector_type: str = CONNECTOR_TYPE

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        rel_path = file_data.source_identifiers.relative_path
        if not rel_path:
            raise ValueError("Missing relative path in source_identifiers.")

        download_path = self.download_dir / Path(rel_path)
        download_path.parent.mkdir(parents=True, exist_ok=True)

        with open(download_path, "w", encoding="utf-8") as f:
            for k, v in file_data.additional_metadata.items():
                f.write(f"{k}: {v}\n")

        return self.generate_download_response(file_data=file_data, download_path=download_path)


servicenow_source_entry = SourceRegistryEntry(
    connection_config=ServiceNowConnectionConfig,
    indexer_config=ServiceNowIndexerConfig,
    indexer=ServiceNowIndexer,
    downloader_config=ServiceNowDownloaderConfig,
    downloader=ServiceNowDownloader,
)
