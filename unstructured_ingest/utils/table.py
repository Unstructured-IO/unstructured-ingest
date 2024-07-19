from typing import Any

import pandas as pd

from unstructured_ingest.utils.data_prep import flatten_dict


def get_default_pandas_dtypes() -> dict[str, Any]:
    return {
        "text": pd.StringDtype(),  # type: ignore
        "type": pd.StringDtype(),  # type: ignore
        "element_id": pd.StringDtype(),  # type: ignore
        "filename": pd.StringDtype(),  # Optional[str]  # type: ignore
        "filetype": pd.StringDtype(),  # Optional[str]  # type: ignore
        "file_directory": pd.StringDtype(),  # Optional[str]  # type: ignore
        "last_modified": pd.StringDtype(),  # Optional[str]  # type: ignore
        "attached_to_filename": pd.StringDtype(),  # Optional[str]  # type: ignore
        "parent_id": pd.StringDtype(),  # Optional[str],  # type: ignore
        "category_depth": "Int64",  # Optional[int]
        "image_path": pd.StringDtype(),  # Optional[str]  # type: ignore
        "languages": object,  # Optional[list[str]]
        "page_number": "Int64",  # Optional[int]
        "page_name": pd.StringDtype(),  # Optional[str]  # type: ignore
        "url": pd.StringDtype(),  # Optional[str]  # type: ignore
        "link_urls": pd.StringDtype(),  # Optional[str]  # type: ignore
        "link_texts": object,  # Optional[list[str]]
        "links": object,
        "sent_from": object,  # Optional[list[str]],
        "sent_to": object,  # Optional[list[str]]
        "subject": pd.StringDtype(),  # Optional[str]  # type: ignore
        "section": pd.StringDtype(),  # Optional[str]  # type: ignore
        "header_footer_type": pd.StringDtype(),  # Optional[str]  # type: ignore
        "emphasized_text_contents": object,  # Optional[list[str]]
        "emphasized_text_tags": object,  # Optional[list[str]]
        "text_as_html": pd.StringDtype(),  # Optional[str]  # type: ignore
        "regex_metadata": object,
        "max_characters": "Int64",  # Optional[int]
        "is_continuation": "boolean",  # Optional[bool]
        "detection_class_prob": float,  # Optional[float],
        "sender": pd.StringDtype(),  # type: ignore
        "coordinates_points": object,
        "coordinates_system": pd.StringDtype(),  # type: ignore
        "coordinates_layout_width": float,
        "coordinates_layout_height": float,
        "data_source_url": pd.StringDtype(),  # Optional[str]  # type: ignore
        "data_source_version": pd.StringDtype(),  # Optional[str]  # type: ignore
        "data_source_record_locator": object,
        "data_source_date_created": pd.StringDtype(),  # Optional[str]  # type: ignore
        "data_source_date_modified": pd.StringDtype(),  # Optional[str]  # type: ignore
        "data_source_date_processed": pd.StringDtype(),  # Optional[str]  # type: ignore
        "data_source_permissions_data": object,
        "embeddings": object,
        "regex_metadata_key": object,
    }


def convert_to_pandas_dataframe(
    elements_dict: list[dict[str, Any]],
    drop_empty_cols: bool = False,
) -> pd.DataFrame:
    # Flatten metadata if it hasn't already been flattened
    for d in elements_dict:
        if metadata := d.pop("metadata", None):
            d.update(flatten_dict(metadata, keys_to_omit=["data_source_record_locator"]))

    df = pd.DataFrame.from_dict(
        elements_dict,
    )
    dt = {k: v for k, v in get_default_pandas_dtypes().items() if k in df.columns}
    df = df.astype(dt)
    if drop_empty_cols:
        df.dropna(axis=1, how="all", inplace=True)
    return df
