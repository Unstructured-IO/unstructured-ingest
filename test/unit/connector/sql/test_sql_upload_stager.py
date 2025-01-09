import json

import pandas as pd

from unstructured_ingest.v2.processes.connectors.sql.sql import SQLUploadStager

test_element = {
    "type": "Text",
    "element_id": "cb869d39d5fadad791c50ef57eda8bfb",
    "text": "some test text",
    "file_directory": "/tmp/files",
    "filename": "some-file.pdf",
    "languages": ["eng"],
    "last_modified": "2024-11-27T15:36:24",
    "page_number": 1,
    "filetype": "application/pdf",
    "url": "s3://some-bucket/some-file.pdf",
    "version": "60598d87b05db06b0f13efbbb69b7e99",
    "record_locator": {
        "protocol": "s3",
        "remote_file_path": "s3://some-bucket/",
    },
    "date_created": "1732718184.0",
    "date_modified": "1732718184.0",
    "date_processed": "1734625322.9292843",
    "points": [
        [108.0, 74.15232159999994],
        [108.0, 95.0239216],
        [505.7402969717998, 95.0239216],
        [505.7402969717998, 74.15232159999994],
    ],
    "system": "PixelSpace",
    "layout_width": 612,
    "layout_height": 792,
    "id": "a658ea27-7c64-55b3-9111-941da4688ea8",
    "record_id": "91c26667-5e97-5dc6-9252-cc54ec6c5cc6",
    "permissions_data": {"read": True, "write": False},
    "regex_metadata": "some regex metadata",
    "parent_id": "91c26667-5e97-5dc6-9252-cc54ec6c5cc6",
    "links": ["https://example.com"],
}
stager = SQLUploadStager()


def test_sql_upload_stager_conform_dataframe_dates():
    df = pd.DataFrame(data=[test_element.copy(), test_element.copy()])
    conformed_df = stager.conform_dataframe(df)
    for column in ["date_created", "date_modified", "date_processed", "last_modified"]:
        assert conformed_df[column].apply(lambda x: isinstance(x, float)).all()


def test_sql_upload_stager_conform_dataframe_json():
    df = pd.DataFrame(data=[test_element.copy(), test_element.copy()])
    conformed_df = stager.conform_dataframe(df)
    for column in ["permissions_data", "record_locator", "points", "links"]:
        assert conformed_df[column].apply(lambda x: isinstance(x, str)).all()
        assert (
            conformed_df[column]
            .apply(lambda x: json.loads(x))
            .apply(lambda x: isinstance(x, (list, dict)))
            .all()
        )


def test_sql_upload_stager_conform_dataframe_strings():
    df = pd.DataFrame(data=[test_element.copy(), test_element.copy()])
    conformed_df = stager.conform_dataframe(df)
    for column in ["version", "page_number", "regex_metadata"]:
        assert conformed_df[column].apply(lambda x: isinstance(x, str)).all()
