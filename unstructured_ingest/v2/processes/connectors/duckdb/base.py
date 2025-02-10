from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from unstructured_ingest.utils.data_prep import get_data, write_data
from unstructured_ingest.v2.interfaces import FileData, UploadStager
from unstructured_ingest.v2.utils import get_enhanced_element_id

_COLUMNS = (
    "id",
    "element_id",
    "text",
    "embeddings",
    "type",
    "system",
    "layout_width",
    "layout_height",
    "points",
    "url",
    "version",
    "date_created",
    "date_modified",
    "date_processed",
    "permissions_data",
    "record_locator",
    "category_depth",
    "parent_id",
    "attached_filename",
    "filetype",
    "last_modified",
    "file_directory",
    "filename",
    "languages",
    "page_number",
    "links",
    "page_name",
    "link_urls",
    "link_texts",
    "sent_from",
    "sent_to",
    "subject",
    "section",
    "header_footer_type",
    "emphasized_text_contents",
    "emphasized_text_tags",
    "text_as_html",
    "regex_metadata",
    "detection_class_prob",
)

# _DATE_COLUMNS = ("date_created", "date_modified", "date_processed", "last_modified")


@dataclass
class BaseDuckDBUploadStager(UploadStager):

    def conform_dict(self, element_dict: dict, file_data: FileData) -> dict:
        data = element_dict.copy()
        metadata: dict[str, Any] = data.pop("metadata", {})
        data_source = metadata.pop("data_source", {})
        coordinates = metadata.pop("coordinates", {})

        data.update(metadata)
        data.update(data_source)
        data.update(coordinates)

        data["id"] = get_enhanced_element_id(element_dict=data, file_data=file_data)

        # remove extraneous, not supported columns
        data = {k: v for k, v in data.items() if k in _COLUMNS}
        return data

    def run(
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        elements_contents = get_data(path=elements_filepath)
        output_filename_suffix = Path(elements_filepath).suffix
        output_filename = f"{Path(output_filename).stem}{output_filename_suffix}"
        output_path = self.get_output_path(output_filename=output_filename, output_dir=output_dir)

        output = [
            self.conform_dict(element_dict=element_dict, file_data=file_data)
            for element_dict in elements_contents
        ]
        df = pd.DataFrame(data=output)

        for column in filter(
            lambda x: x in df.columns,
            ("version", "page_number", "regex_metadata"),
        ):
            df[column] = df[column].apply(str)

        data = df.to_dict(orient="records")
        write_data(path=output_path, data=data)
        return output_path
