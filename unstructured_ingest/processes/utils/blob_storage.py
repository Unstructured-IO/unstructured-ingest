from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.interfaces import UploadStager, UploadStagerConfig


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
        # Always emit .json. Stream the copy element-by-element instead of
        # get_json_data + write_data (which loaded the whole file and held 3+ copies
        # resident, OOM-killing the stager on large partition outputs). The base
        # process_whole streams a .json array via json_stream + write_data_streaming,
        # with a buffered fallback for the rare ijson-incompatible inputs; conform_dict
        # is identity here, so this stays a pure format copy.
        output_file = self.get_output_path(
            output_filename=output_filename, output_dir=output_dir
        ).with_suffix(".json")
        self.process_whole(
            input_file=elements_filepath, output_file=output_file, file_data=file_data
        )
        return output_file
