import itertools
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, Generator, Iterable, Optional, Sequence, TypeVar, cast
from uuid import NAMESPACE_DNS, uuid5

import ijson

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.logger import logger
from unstructured_ingest.utils import ndjson
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from pandas import DataFrame

DATE_FORMATS = ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d+%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z")

T = TypeVar("T")
IterableT = Iterable[T]


def split_dataframe(df: "DataFrame", chunk_size: int = 100) -> Generator["DataFrame", None, None]:
    # range(0, len(df), chunk_size) never produces an out-of-bounds slice, so
    # every yielded chunk has at least one row. The previous num_chunks+1
    # formula yielded a spurious empty trailing chunk whenever len(df) was an
    # exact multiple of chunk_size, which caused teradatasql to send an empty
    # executemany batch to the server and receive Error 3939.
    if len(df) == 0:
        return
    for i in range(0, len(df), chunk_size):
        yield df[i : i + chunk_size]


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
            import pandas as pd

            df = pd.read_csv(path)
            return df.to_dict(orient="records")
        elif path.suffix == ".parquet":
            import pandas as pd

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


def json_stream(path: Path) -> Generator[dict, None, None]:
    """Yield the elements of a top-level JSON array one at a time.

    This is the bounded-memory equivalent of ``json.load(path)`` for a file whose
    root is a JSON array of objects (the element file format produced by the
    partition step). Only one element is resident at a time, so an arbitrarily
    large file is processed in roughly flat memory.

    ``use_float=True`` makes ijson yield native ``float``/``int`` values instead of
    ``decimal.Decimal``, matching what ``json.load`` would have produced so the
    elements round-trip identically through ``json.dumps``.

    NOTE: ijson is stricter than the stdlib ``json`` module. It raises
    ``ijson.JSONError`` on inputs ``json.load`` accepts: the non-standard constants
    ``NaN`` / ``Infinity`` / ``-Infinity`` and integers larger than int64 (the yajl
    C backend overflows). Callers that need full ``json.load`` compatibility should
    catch ``ijson.JSONError`` and fall back to a buffered read (see
    ``UploadStager.process_whole``).
    """
    with path.open("rb") as f:
        yield from ijson.items(f, "item", use_float=True)


def _write_json_array_stream(f: "IO[str]", data: Iterable[dict], indent: Optional[int]) -> None:
    """Stream-write ``data`` as a JSON array, byte-for-byte identical to
    ``json.dump(list(data), f, indent=indent, ensure_ascii=False)`` but without
    materializing the list."""
    if indent is None:
        f.write("[")
        for i, element in enumerate(data):
            if i:
                f.write(", ")
            f.write(json.dumps(element, ensure_ascii=False))
        f.write("]")
        return

    pad = " " * indent
    wrote_any = False
    for i, element in enumerate(data):
        f.write("[\n" if i == 0 else ",\n")
        wrote_any = True
        chunk = json.dumps(element, indent=indent, ensure_ascii=False)
        f.write("\n".join(f"{pad}{line}" for line in chunk.split("\n")))
    f.write("\n]" if wrote_any else "[]")


def _write_ndjson_stream(f: "IO[str]", data: Iterable[dict]) -> None:
    """Stream-write ``data`` as newline-delimited JSON, byte-for-byte identical to
    ``ndjson.dump(list(data), f, ensure_ascii=False)`` but without materializing
    the list."""
    for i, element in enumerate(data):
        if i:
            f.write("\n")
        f.write(json.dumps(element, ensure_ascii=False))


def write_data_streaming(path: Path, data: Iterable[dict], indent: Optional[int] = 2) -> None:
    """Bounded-memory drop-in for :func:`write_data` that accepts an iterable/generator.

    Produces output identical to ``write_data`` for the same elements while only
    holding a single element in memory at a time, so it pairs with
    :func:`json_stream` to copy/transform arbitrarily large element files without
    loading them whole.

    The write is ATOMIC: output is written to a temp file in the same directory and
    only ``os.replace``-d into ``path`` once the iterable has been fully consumed
    without error. This is required because the stager is routinely called with the
    output path equal to the input path; a plain ``open(path, "w")`` would truncate
    the input file before the lazy ``json_stream`` generator had finished reading it,
    corrupting the stream mid-parse. Writing to a temp file first leaves the input
    intact until the generator is exhausted. It also means that if the producing
    generator raises mid-stream (a parse error or a per-element transform failure),
    the temp file is discarded and any existing file at ``path`` is left untouched,
    rather than replacing a known-good artifact with a corrupt partial file.
    """
    if path.suffix not in (".json", ".ndjson"):
        raise IOError(f"Unsupported file type: {path}")

    # mkstemp creates the temp file as 0600; os.replace would then carry that over and
    # narrow an existing destination's permissions, breaking shared-read workflows.
    # Preserve the existing file's mode, or fall back to a world-readable default for
    # new files (the previous plain open(path, "w") left perms at the process umask
    # default; 0644 matches the common case without the umask race in this async path).
    existing_mode = path.stat().st_mode & 0o777 if path.exists() else None
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w") as f:
            if path.suffix == ".json":
                _write_json_array_stream(f=f, data=data, indent=indent)
            else:
                _write_ndjson_stream(f=f, data=data)
        os.chmod(tmp_path, existing_mode if existing_mode is not None else 0o644)
        os.replace(tmp_path, path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


def get_json_data(path: Path) -> list[dict]:
    with path.open() as f:
        # Attempt by prefix
        if path.suffix == ".json":
            return json.load(f)
        elif path.suffix == ".ndjson":
            return ndjson.load(f)
        try:
            return json.load(f)
        except Exception as e:
            logger.warning(f"failed to read {path} as json: {e}")
            f.seek(0)
        try:
            return ndjson.load(f)
        except Exception as e:
            logger.warning(f"failed to read {path} as ndjson: {e}")
    raise ValueError(f"Unsupported json file: {path}")


@requires_dependencies(["pandas"])
def get_data_df(path: Path) -> "DataFrame":
    import pandas as pd

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


def get_enhanced_element_id(element_dict: dict, file_data: FileData) -> str:
    element_id = element_dict.get("element_id")
    new_data = f"{element_id}{file_data.identifier}"
    return str(uuid5(NAMESPACE_DNS, new_data))
