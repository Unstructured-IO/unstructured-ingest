import json
from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

import ndjson
from pydantic import BaseModel

from unstructured_ingest.v2.interfaces.file_data import FileData
from unstructured_ingest.v2.interfaces.process import BaseProcess


class UploadStagerConfig(BaseModel):
    pass


UploadStagerConfigT = TypeVar("UploadStagerConfigT", bound=UploadStagerConfig)


@dataclass
class UploadStager(BaseProcess, ABC):
    upload_stager_config: UploadStagerConfigT

    def write_output(self, output_path: Path, data: list[dict]) -> None:
        if output_path.suffix == ".json":
            with output_path.open("w") as f:
                json.dump(data, f, indent=2)
        elif output_path.suffix == ".ndjson":
            with output_path.open("w") as f:
                ndjson.dump(data, f)
        else:
            raise ValueError(f"Unsupported output format: {output_path}")

    def get_data(self, elements_filepath: Path) -> list[dict]:
        if elements_filepath.suffix == ".json":
            with elements_filepath.open() as f:
                return json.load(f)
        elif elements_filepath.suffix == ".ndjson":
            with elements_filepath.open() as f:
                return ndjson.load(f)
        else:
            raise ValueError(f"Unsupported input format: {elements_filepath}")

    def conform_dict(self, element_dict: dict, file_data: FileData) -> dict:
        return element_dict

    def get_output_path(self, output_filename: str, output_dir: Path) -> Path:
        output_path = Path(output_filename)
        output_filename = f"{Path(output_filename).stem}{output_path.suffix}"
        output_path = Path(output_dir) / Path(f"{output_filename}")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path

    def stream_update(self, input_file: Path, output_file: Path, file_data: FileData) -> None:
        with input_file.open() as in_f:
            reader = ndjson.reader(in_f)
            with output_file.open("w") as out_f:
                writer = ndjson.writer(out_f)
                for element in reader:
                    conformed_element = self.conform_dict(element_dict=element, file_data=file_data)
                    writer.writerow(row=conformed_element)
                    writer.f.flush()

    def process_whole(self, input_file: Path, output_file: Path, file_data: FileData) -> None:
        with input_file.open() as in_f:
            elements_contents = json.load(in_f)

        conformed_elements = [
            self.conform_dict(element_dict=element, file_data=file_data)
            for element in elements_contents
        ]

        with open(output_file, "w") as out_f:
            json.dump(conformed_elements, out_f, indent=2)

    def run(
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        output_file = self.get_output_path(output_filename=output_filename, output_dir=output_dir)
        if elements_filepath.suffix == ".ndjson":
            self.stream_update(
                input_file=elements_filepath, output_file=output_file, file_data=file_data
            )
        elif elements_filepath.suffix == ".json":
            self.process_whole(
                input_file=elements_filepath, output_file=output_file, file_data=file_data
            )
        else:
            raise ValueError(f"Unsupported file extension: {elements_filepath}")
        return output_file

    async def run_async(
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        return self.run(
            elements_filepath=elements_filepath,
            output_dir=output_dir,
            output_filename=output_filename,
            file_data=file_data,
            **kwargs,
        )
