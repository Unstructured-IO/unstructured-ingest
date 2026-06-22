from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

import ijson
from pydantic import BaseModel

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.interfaces import BaseProcess
from unstructured_ingest.logger import logger
from unstructured_ingest.utils import ndjson
from unstructured_ingest.utils.data_prep import (
    get_json_data,
    json_stream,
    write_data,
    write_data_streaming,
)


class UploadStagerConfig(BaseModel):
    pass


UploadStagerConfigT = TypeVar("UploadStagerConfigT", bound=UploadStagerConfig)


@dataclass
class UploadStager(BaseProcess, ABC):
    upload_stager_config: UploadStagerConfigT

    def conform_dict(self, element_dict: dict, file_data: FileData) -> dict:
        return element_dict

    def should_include(self, element_dict: dict) -> bool:
        """Return False to exclude an element from the staged output.
        Default behavior includes every element. Subclasses may override
        to filter out elements that the destination cannot accept."""
        return True

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
                    # Filter hook: default returns True so existing behavior is preserved;
                    # subclasses override should_include() to drop elements the destination
                    # cannot accept.
                    if not self.should_include(element_dict=element):
                        continue
                    conformed_element = self.conform_dict(element_dict=element, file_data=file_data)
                    writer.write(row=conformed_element)
                    writer.f.flush()

    def process_whole(self, input_file: Path, output_file: Path, file_data: FileData) -> None:
        # Stream the JSON array element-by-element rather than loading the whole
        # file into memory. A very large partition output can be several hundred MB;
        # json.load + a full conformed list + write_data held 3+ copies resident
        # and OOM-killed the stager. json_stream/write_data_streaming keep only a
        # single element resident at a time, matching the bounded-memory profile
        # of the .ndjson stream_update path.
        try:
            self._process_whole_streaming(
                input_file=input_file, output_file=output_file, file_data=file_data
            )
        except (ijson.JSONError, OverflowError):
            # ijson rejects a few inputs that Python's stdlib json module accepts
            # and that legitimately appear in element files: the non-standard
            # constants NaN / Infinity / -Infinity (ijson.JSONError), and integers
            # larger than int64 (the yajl C backend overflows, surfacing as
            # OverflowError). The legacy path used json.load, which
            # tolerates all of these, so fall back to the buffered whole-file path
            # to preserve behavior. This file loses the memory bound, exactly as the
            # legacy path did; a genuinely malformed file re-raises from json.load.
            logger.warning(
                f"streaming parse of {input_file} failed on a json.load-compatible "
                "extension (NaN/Infinity or >int64 integer); falling back to a "
                "buffered whole-file read for this file."
            )
            self._process_whole_buffered(
                input_file=input_file, output_file=output_file, file_data=file_data
            )

    def _process_whole_streaming(
        self, input_file: Path, output_file: Path, file_data: FileData
    ) -> None:
        # Filter hook: default returns True so existing behavior is preserved;
        # subclasses override should_include() to drop elements the destination
        # cannot accept.
        conformed_elements = (
            self.conform_dict(element_dict=element, file_data=file_data)
            for element in json_stream(path=input_file)
            if self.should_include(element_dict=element)
        )
        write_data_streaming(path=output_file, data=conformed_elements)

    def _process_whole_buffered(
        self, input_file: Path, output_file: Path, file_data: FileData
    ) -> None:
        # This path is only reached when ijson rejected the input (NaN/Infinity/
        # >int64), so it already gives up the memory bound, exactly like the legacy
        # whole-file path did. get_json_data reads the entire input into memory
        # *before* write_data opens the output, so it stays correct even when
        # output_file == input_file (the in-place stage case): the read completes
        # before the write truncates. write_data itself is not atomic, matching
        # every other write_data caller in the codebase; a genuinely corrupt input
        # re-raises here from json.load rather than being silently swallowed.
        elements_contents = get_json_data(path=input_file)
        conformed_elements = [
            self.conform_dict(element_dict=element, file_data=file_data)
            for element in elements_contents
            if self.should_include(element_dict=element)
        ]
        write_data(path=output_file, data=conformed_elements)

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
