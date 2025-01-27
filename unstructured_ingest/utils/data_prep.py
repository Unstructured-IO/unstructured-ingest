import itertools
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Iterable, Optional, Sequence, TypeVar, cast

import pandas as pd

from unstructured_ingest.utils import ndjson
from unstructured_ingest.v2.logger import logger

DATE_FORMATS = ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d+%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z")

T = TypeVar("T")
IterableT = Iterable[T]


def split_dataframe(df: pd.DataFrame, chunk_size: int = 100) -> Generator[pd.DataFrame, None, None]:
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        yield df[i * chunk_size : (i + 1) * chunk_size]


def batch_generator(iterable: IterableT, batch_size: int = 100) -> IterableT:
    """A helper function to break an iterable into batches of size batch_size."""
    it = iter(iterable)
    chunk = tuple(itertools.islice(it, batch_size))
    while chunk:
        yield chunk
        chunk = tuple(itertools.islice(it, batch_size))


def generator_batching_wbytes(
    iterable: IterableT,
    batch_size_limit_bytes: Optional[int] = None,
    max_batch_size: Optional[int] = None,
) -> IterableT:
    if not batch_size_limit_bytes and not max_batch_size:
        return iterable
    """A helper function to break an iterable into chunks of specified bytes."""
    current_batch, current_batch_size = [], 0

    for item in iterable:
        item_size_bytes = len(json.dumps(item).encode("utf-8"))
        if batch_size_limit_bytes and current_batch_size + item_size_bytes > batch_size_limit_bytes:
            yield current_batch
            current_batch, current_batch_size = [item], item_size_bytes
            continue
        if max_batch_size and len(current_batch) + 1 > max_batch_size:
            yield current_batch
            current_batch, current_batch_size = [item], item_size_bytes
            continue

        current_batch.append(item)
        current_batch_size += item_size_bytes

    if current_batch:
        yield current_batch


def flatten_dict(
    dictionary: dict[str, Any],
    parent_key: str = "",
    separator: str = "_",
    flatten_lists: bool = False,
    remove_none: bool = False,
    keys_to_omit: Optional[Sequence[str]] = None,
) -> dict[str, Any]:
    """Flattens a nested dictionary into a single level dictionary.

    keys_to_omit is a list of keys that don't get flattened. If omitting a nested key, format as
    {parent_key}{separator}{key}. If flatten_lists is True, then lists and tuples are flattened as
    well. If remove_none is True, then None keys/values are removed from the flattened
    dictionary.
    """
    keys_to_omit = keys_to_omit if keys_to_omit else []
    flattened_dict: dict[str, Any] = {}
    for key, value in dictionary.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        if new_key in keys_to_omit:
            flattened_dict[new_key] = value
        elif value is None and remove_none:
            continue
        elif isinstance(value, dict):
            value = cast("dict[str, Any]", value)
            flattened_dict.update(
                flatten_dict(
                    value, new_key, separator, flatten_lists, remove_none, keys_to_omit=keys_to_omit
                ),
            )
        elif isinstance(value, (list, tuple)) and flatten_lists:
            value = cast("list[Any] | tuple[Any]", value)
            for index, item in enumerate(value):
                flattened_dict.update(
                    flatten_dict(
                        {f"{new_key}{separator}{index}": item},
                        "",
                        separator,
                        flatten_lists,
                        remove_none,
                        keys_to_omit=keys_to_omit,
                    )
                )
        else:
            flattened_dict[new_key] = value

    return flattened_dict


def validate_date_args(date: Optional[str] = None) -> bool:
    """Validate whether the provided date string satisfies any of the supported date formats.

    Used by unstructured/ingest/connector/biomed.py

    Returns `True` if the date string satisfies any of the supported formats, otherwise raises
    `ValueError`.

    Supported Date Formats:
        - 'YYYY-MM-DD'
        - 'YYYY-MM-DDTHH:MM:SS'
        - 'YYYY-MM-DD+HH:MM:SS'
        - 'YYYY-MM-DDTHH:MM:SS±HHMM'
    """
    if not date:
        raise ValueError("The argument date is None.")

    for format in DATE_FORMATS:
        try:
            datetime.strptime(date, format)
            return True
        except ValueError:
            pass

    raise ValueError(
        f"The argument {date} does not satisfy the format:"
        f" YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD+HH:MM:SS or YYYY-MM-DDTHH:MM:SS±HHMM",
    )


def get_data_by_suffix(path: Path) -> list[dict]:
    with path.open() as f:
        if path.suffix == ".json":
            return json.load(f)
        elif path.suffix == ".ndjson":
            return ndjson.load(f)
        elif path.suffix == ".csv":
            df = pd.read_csv(path)
            return df.to_dict(orient="records")
        elif path.suffix == ".parquet":
            df = pd.read_parquet(path)
            return df.to_dict(orient="records")
        else:
            raise ValueError(f"Unsupported file type: {path}")


def write_data(path: Path, data: list[dict], indent: Optional[int] = 2) -> None:
    with path.open("w") as f:
        if path.suffix == ".json":
            json.dump(data, f, indent=indent, ensure_ascii=False)
        elif path.suffix == ".ndjson":
            ndjson.dump(data, f, ensure_ascii=False)
        else:
            raise IOError("Unsupported file type: {path}")


def get_data(path: Path) -> list[dict]:
    try:
        return get_data_by_suffix(path=path)
    except Exception as e:
        logger.warning(f"failed to read {path} by extension: {e}")
    # Fall back
    with path.open() as f:
        try:
            return json.load(f)
        except Exception as e:
            logger.warning(f"failed to read {path} as json: {e}")
        try:
            return ndjson.load(f)
        except Exception as e:
            logger.warning(f"failed to read {path} as ndjson: {e}")
        try:
            df = pd.read_csv(path)
            return df.to_dict(orient="records")
        except Exception as e:
            logger.warning(f"failed to read {path} as csv: {e}")
        try:
            df = pd.read_parquet(path)
            return df.to_dict(orient="records")
        except Exception as e:
            logger.warning(f"failed to read {path} as parquet: {e}")


def get_data_df(path: Path) -> pd.DataFrame:
    with path.open() as f:
        if path.suffix == ".json":
            data = json.load(f)
            return pd.DataFrame(data=data)
        elif path.suffix == ".ndjson":
            data = ndjson.load(f)
            return pd.DataFrame(data=data)
        elif path.suffix == ".csv":
            df = pd.read_csv(path)
            return df
        elif path.suffix == ".parquet":
            df = pd.read_parquet(path)
            return df
        else:
            raise ValueError(f"Unsupported file type: {path}")
