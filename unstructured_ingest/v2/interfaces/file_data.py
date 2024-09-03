import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Optional

from dataclasses_json import DataClassJsonMixin


@dataclass
class SourceIdentifiers:
    filename: str
    fullpath: str
    rel_path: Optional[str] = None

    @property
    def filename_stem(self) -> str:
        return Path(self.filename).stem

    @property
    def relative_path(self) -> str:
        return self.rel_path or self.fullpath


@dataclass
class FileDataSourceMetadata(DataClassJsonMixin):
    url: Optional[str] = None
    version: Optional[str] = None
    record_locator: Optional[dict[str, Any]] = None
    date_created: Optional[str] = None
    date_modified: Optional[str] = None
    date_processed: Optional[str] = None
    permissions_data: Optional[list[dict[str, Any]]] = None
    filesize_bytes: Optional[int] = None


@dataclass
class FileData(DataClassJsonMixin):
    identifier: str
    connector_type: str
    source_identifiers: Optional[SourceIdentifiers] = None
    doc_type: Literal["file", "batch"] = field(default="file")
    metadata: FileDataSourceMetadata = field(default_factory=lambda: FileDataSourceMetadata())
    additional_metadata: dict[str, Any] = field(default_factory=dict)
    reprocess: bool = False
    local_download_path: Optional[str] = None

    @classmethod
    def from_file(cls, path: str) -> "FileData":
        path = Path(path).resolve()
        if not path.exists() or not path.is_file():
            raise ValueError(f"file path not valid: {path}")
        with open(str(path.resolve()), "rb") as f:
            file_data_dict = json.load(f)
        file_data = FileData.from_dict(file_data_dict)
        return file_data

    def to_file(self, path: str) -> None:
        path = Path(path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(str(path.resolve()), "w") as f:
            json.dump(self.to_dict(), f, indent=2)
