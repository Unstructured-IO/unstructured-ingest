from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.interfaces import UploadStager, UploadStagerConfig
from unstructured_ingest.utils import ndjson
from unstructured_ingest.utils.data_prep import write_data_streaming


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
        # Always emit .json. NDJSON input is streamed element-by-element into a JSON
        # array; JSON-array input goes through process_whole (streaming with a buffered
        # fallback). Both paths keep only one element resident at a time.
        output_file = self.get_output_path(
            output_filename=output_filename, output_dir=output_dir
        ).with_suffix(".json")
        if elements_filepath.suffix == ".ndjson":
            with elements_filepath.open() as input_file:
                write_data_streaming(path=output_file, data=ndjson.reader(input_file))
        elif elements_filepath.suffix == ".json":
            self.process_whole(
                input_file=elements_filepath, output_file=output_file, file_data=file_data
            )
        else:
            raise ValueError(f"Unsupported file extension: {elements_filepath}")
        return output_file
