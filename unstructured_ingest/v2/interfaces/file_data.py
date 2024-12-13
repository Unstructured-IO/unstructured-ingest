import json
from pathlib import Path
from typing import Any, Optional
from uuid import NAMESPACE_DNS, uuid5

from pydantic import BaseModel, Field, field_validator, model_validator


class SourceIdentifiers(BaseModel):
    filename: str
    fullpath: str
    rel_path: Optional[str] = None

    @property
    def filename_stem(self) -> str:
        return Path(self.filename).stem

    @property
    def relative_path(self) -> str:
        return self.rel_path or self.fullpath


class FileDataSourceMetadata(BaseModel):
    url: Optional[str] = None
    version: Optional[str] = None
    record_locator: Optional[dict[str, Any]] = None
    date_created: Optional[str] = None
    date_modified: Optional[str] = None
    date_processed: Optional[str] = None
    permissions_data: Optional[list[dict[str, Any]]] = None
    filesize_bytes: Optional[int] = None


class FileData(BaseModel):
    identifier: str
    connector_type: str
    source_identifiers: Optional[SourceIdentifiers] = None
    metadata: FileDataSourceMetadata = Field(default_factory=lambda: FileDataSourceMetadata())
    additional_metadata: dict[str, Any] = Field(default_factory=dict)
    reprocess: bool = False
    local_download_path: Optional[str] = None
    display_name: Optional[str] = None

    @classmethod
    def from_file(cls, path: str) -> "FileData":
        path = Path(path).resolve()
        if not path.exists() or not path.is_file():
            raise ValueError(f"file path not valid: {path}")
        with open(str(path.resolve()), "rb") as f:
            file_data_dict = json.load(f)
        file_data = FileData.model_validate(file_data_dict)
        return file_data

    @classmethod
    def cast(cls, file_data: "FileData") -> "FileData":
        file_data_dict = file_data.model_dump()
        return cls.model_validate(file_data_dict)

    def to_file(self, path: str) -> None:
        path = Path(path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(str(path.resolve()), "w") as f:
            json.dump(self.model_dump(), f, indent=2)


class BatchItem(BaseModel):
    identifier: str
    version: Optional[str] = None


class BatchFileData(FileData):
    identifier: str = Field(init=False)
    batch_items: list[BatchItem]

    @field_validator("batch_items")
    @classmethod
    def check_uniqueness(cls, v: list[BatchItem]) -> list[BatchItem]:
        if not v:
            raise ValueError("batch items cannot be empty")
        all_identifiers = [item.identifier for item in v]
        if len(all_identifiers) != len(set(all_identifiers)):
            raise ValueError(f"duplicate identifiers: {all_identifiers}")
        sorted_batch_items = sorted(v, key=lambda item: item.identifier)
        return sorted_batch_items

    @model_validator(mode="before")
    @classmethod
    def populate_identifier(cls, data: Any) -> Any:
        if isinstance(data, dict) and "identifier" not in data:
            batch_items = data["batch_items"]
            identifier_data = json.dumps(
                {item.identifier: item.version for item in batch_items}, sort_keys=True
            )
            data["identifier"] = str(uuid5(NAMESPACE_DNS, str(identifier_data)))
        return data
