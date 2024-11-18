from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional
from uuid import NAMESPACE_DNS, uuid5

import pandas
from pydantic import BaseModel, Field, Secret, field_validator

from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    FileData,
    Indexer,
    IndexerConfig,
    SourceIdentifiers,
)
from unstructured_ingest.v2.processes.connector_registry import (
    SourceRegistryEntry,
)

if TYPE_CHECKING:
    from pyairtable import Api
    from pyairtable.api.types import RecordDict

CONNECTOR_TYPE = "airtable"


class AirtableTableMeta(BaseModel):
    """Metadata specifying a table id, a base id which the table is stored in,
    and an t.Optional view id in case particular rows and fields are to be ingested"""

    base_id: str
    table_id: str
    view_id: Optional[str] = None

    def get_id(self) -> str:
        id_s = f"{self.base_id}{self.table_id}"
        id_s = f"{id_s}{self.view_id}" if self.view_id else id_s
        return str(uuid5(NAMESPACE_DNS, id_s))


class AirtableAccessConfig(AccessConfig):
    personal_access_token: str = Field(
        description="Personal access token to authenticate into Airtable. Check: "
        "https://support.airtable.com/docs/creating-and-using-api-keys-and-access-tokens "
        "for more info"
    )


class AirtableConnectionConfig(ConnectionConfig):
    access_config: Secret[AirtableAccessConfig]

    @requires_dependencies(["pyairtable"], extras="airtable")
    def get_client(self) -> "Api":
        from pyairtable import Api

        access_config = self.access_config.get_secret_value()
        return Api(api_key=access_config.personal_access_token)


class AirtableIndexerConfig(IndexerConfig):
    list_of_paths: Optional[list[str]] = Field(
        default=None,
        description="""
        A list of paths that specify the locations to ingest data from within Airtable.

        If this argument is not set, the connector ingests all tables within each and every base.
        --list-of-paths: path1 path2 path3 ….
        path: base_id/table_id(optional)/view_id(optional)/

        To obtain (base, table, view) ids in bulk, check:
        https://airtable.com/developers/web/api/list-bases (base ids)
        https://airtable.com/developers/web/api/get-base-schema (table and view ids)
        https://pyairtable.readthedocs.io/en/latest/metadata.html (base, table and view ids)

        To obtain specific ids from Airtable UI, go to your workspace, and copy any
        relevant id from the URL structure:
        https://airtable.com/appAbcDeF1ghijKlm/tblABcdEfG1HIJkLm/viwABCDEfg6hijKLM
        appAbcDeF1ghijKlm -> base_id
        tblABcdEfG1HIJkLm -> table_id
        viwABCDEfg6hijKLM -> view_id

        You can also check: https://support.airtable.com/docs/finding-airtable-ids

        Here is an example for one --list-of-paths:
            base1/		→ gets the entirety of all tables inside base1
            base1/table1		→ gets all rows and columns within table1 in base1
            base1/table1/view1	→ gets the rows and columns that are
                                  visible in view1 for the table1 in base1

        Examples to invalid airtable_paths:
            table1          → has to mention base to be valid
            base1/view1     → has to mention table to be valid
                """,
    )

    @classmethod
    def validate_path(cls, path: str):
        components = path.split("/")
        if len(components) > 3:
            raise ValueError(
                f"Path must be of the format: base_id/table_id/view_id, "
                f"where table id and view id are optional. Got: {path}"
            )

    @field_validator("list_of_paths")
    @classmethod
    def validate_format(cls, v: list[str]) -> list[str]:
        for path in v:
            cls.validate_path(path=path)
        return v


@dataclass
class AirtableIndexer(Indexer):
    connector_type: str = CONNECTOR_TYPE
    connection_config: AirtableConnectionConfig
    index_config: AirtableIndexerConfig

    def get_all_table_meta(self) -> list[AirtableTableMeta]:
        client = self.connection_config.get_client()
        bases = client.bases()
        airtable_meta = []
        for base in bases:
            for table in base.schema().tables:
                airtable_meta.append(AirtableTableMeta(base_id=base.id, table_id=table.id))
        return airtable_meta

    def get_base_tables_meta(self, base_id: str) -> list[AirtableTableMeta]:
        client = self.connection_config.get_client()
        base = client.base(base_id=base_id)
        airtable_meta = []
        for table in base.tables():
            airtable_meta.append(AirtableTableMeta(base_id=base.id, table_id=table.id))
        return airtable_meta

    def get_meta_from_list(self) -> list[AirtableTableMeta]:
        airtable_meta = []
        for path in self.index_config.list_of_paths:
            components = path.split("/")
            if len(components) == 1:
                airtable_meta.extend(self.get_base_tables_meta(base_id=components[0]))
            elif len(components) == 2:
                airtable_meta.append(
                    AirtableTableMeta(base_id=components[0], table_id=components[1])
                )
            elif len(components) == 3:
                airtable_meta.append(
                    AirtableTableMeta(
                        base_id=components[0], table_id=components[1], view_id=components[2]
                    )
                )
            else:
                raise ValueError(
                    f"Path must be of the format: base_id/table_id/view_id, "
                    f"where table id and view id are optional. Got: {path}"
                )
        return airtable_meta

    def get_table_metas(self) -> list[AirtableTableMeta]:
        if not self.index_config.list_of_paths:
            return self.get_all_table_meta()
        return self.get_meta_from_list()

    def precheck(self) -> None:
        client = self.connection_config.get_client()
        client.request(method="HEAD", url=client.build_url("meta", "bases"))

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        table_metas = self.get_table_metas()
        for table_meta in table_metas:
            fullpath = (
                f"{table_meta.base_id}/{table_meta.table_id}/{table_meta.view_id}.csv"
                if table_meta.view_id
                else f"{table_meta.base_id}/{table_meta.table_id}.csv"
            )
            yield FileData(
                identifier=table_meta.get_id(),
                connector_type=CONNECTOR_TYPE,
                additional_metadata=table_meta.model_dump(),
                source_identifiers=SourceIdentifiers(
                    filename=str(Path(fullpath).name),
                    fullpath=fullpath,
                ),
            )


class AirtableDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class AirtableDownloader(Downloader):
    connection_config: AirtableConnectionConfig
    download_config: AirtableDownloaderConfig = field(default_factory=AirtableDownloaderConfig)
    connector_type: str = CONNECTOR_TYPE

    def get_table_contents(self, table_meta: AirtableTableMeta) -> list["RecordDict"]:
        client = self.connection_config.get_client()
        table = client.table(base_id=table_meta.base_id, table_name=table_meta.table_id)
        table_fetch_kwargs = {"view": table_meta.view_id} if table_meta.view_id else {}
        rows = table.all(**table_fetch_kwargs)
        return rows

    def _table_row_to_dict(self, table_row: "RecordDict") -> dict:
        row_dict = {
            "id": table_row["id"],
            "created_time": table_row["createdTime"],
        }
        row_dict.update(table_row["fields"])
        return row_dict

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        table_meta = AirtableTableMeta.model_validate(file_data.additional_metadata)
        table_contents = self.get_table_contents(table_meta=table_meta)
        df = pandas.DataFrame.from_dict(
            data=[self._table_row_to_dict(table_row=row) for row in table_contents]
        ).sort_index(axis=1)
        download_path = self.get_download_path(file_data=file_data)
        download_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path_or_buf=download_path)
        return self.generate_download_response(file_data=file_data, download_path=download_path)


airtable_source_entry = SourceRegistryEntry(
    indexer=AirtableIndexer,
    indexer_config=AirtableIndexerConfig,
    downloader=AirtableDownloader,
    downloader_config=AirtableDownloaderConfig,
    connection_config=AirtableConnectionConfig,
)
