from abc import ABC
from copy import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_DNS, uuid5

from pydantic import BaseModel

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.interfaces.process import BaseProcess
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.compression import TAR_FILE_EXT, ZIP_FILE_EXT, uncompress_file


class UncompressConfig(BaseModel):
    pass


@dataclass
class Uncompressor(BaseProcess, ABC):
    config: UncompressConfig = field(default_factory=UncompressConfig)

    def is_async(self) -> bool:
        return True

    def run(self, file_data: FileData, **kwargs: Any) -> list[FileData]:
        local_filepath = Path(file_data.local_download_path)
        if local_filepath.suffix not in TAR_FILE_EXT + ZIP_FILE_EXT:
            return [file_data]
        new_path = uncompress_file(filename=str(local_filepath))
        new_files = [i for i in Path(new_path).rglob("*") if i.is_file()]
        responses = []
        logger.debug(
            "uncompressed {} files from original file {}: {}".format(
                len(new_files), local_filepath, ", ".join([str(f) for f in new_files])
            )
        )
        for f in new_files:
            new_file_data = copy(file_data)
            new_file_data.identifier = str(uuid5(NAMESPACE_DNS, str(f)))
            new_file_data.local_download_path = str(f.resolve())
            new_rel_download_path = str(f).replace(str(Path(local_filepath.parent)), "")[1:]
            new_file_data.source_identifiers = SourceIdentifiers(
                filename=f.name,
                fullpath=str(file_data.source_identifiers.fullpath).replace(
                    file_data.source_identifiers.filename, new_rel_download_path
                ),
                rel_path=(
                    str(file_data.source_identifiers.rel_path).replace(
                        file_data.source_identifiers.filename, new_rel_download_path
                    )
                    if file_data.source_identifiers.rel_path
                    else None
                ),
            )
            responses.append(new_file_data)
        return responses

    async def run_async(self, file_data: FileData, **kwargs: Any) -> list[FileData]:
        return self.run(file_data=file_data, **kwargs)
