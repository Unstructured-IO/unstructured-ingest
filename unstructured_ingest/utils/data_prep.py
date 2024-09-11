import itertools
import json
from datetime import datetime
from typing import Any, Iterable, Optional, Sequence, TypeVar, cast

DATE_FORMATS = ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d+%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z")

T = TypeVar("T")
IterableT = Iterable[T]


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
