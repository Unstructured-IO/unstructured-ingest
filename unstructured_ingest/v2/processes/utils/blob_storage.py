from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from unstructured_ingest.utils.data_prep import get_data, write_data
from unstructured_ingest.v2.interfaces import FileData, UploadStager, UploadStagerConfig


class BlobStoreUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class BlobStoreUploadStager(UploadStager):
    upload_stager_config: BlobStoreUploadStagerConfig = field(
        default_factory=BlobStoreUploadStagerConfig
    )

    def run(
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        output_file = self.get_output_path(output_filename=output_filename, output_dir=output_dir)
        # Always save as json
        data = get_data(elements_filepath)
        write_data(path=output_file.with_suffix(".json"), data=data)
        return output_file.with_suffix(".json")
