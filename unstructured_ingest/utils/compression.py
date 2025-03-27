import os
import sys
import tarfile
import zipfile
from pathlib import Path
from typing import Optional

from unstructured_ingest.logger import logger

ZIP_FILE_EXT = [".zip"]
TAR_FILE_EXT = [".tar", ".tar.gz", ".tgz"]


def uncompress_file(filename: str, path: Optional[str] = None) -> str:
    """
    Takes in a compressed zip or tar file and decompresses it
    """
    # Create path if it doesn't already exist
    if path:
        Path(path).mkdir(parents=True, exist_ok=True)

    if any(filename.endswith(ext) for ext in ZIP_FILE_EXT):
        return uncompress_zip_file(zip_filename=filename, path=path)
    elif any(filename.endswith(ext) for ext in TAR_FILE_EXT):
        return uncompress_tar_file(tar_filename=filename, path=path)
    else:
        raise ValueError(
            "filename {} not a recognized compressed extension: {}".format(
                filename,
                ", ".join(ZIP_FILE_EXT + TAR_FILE_EXT),
            ),
        )


def uncompress_zip_file(zip_filename: str, path: Optional[str] = None) -> str:
    head, tail = os.path.split(zip_filename)
    for ext in ZIP_FILE_EXT:
        if tail.endswith(ext):
            tail = tail[: -(len(ext))]
            break
    path = path if path else os.path.join(head, f"{tail}-zip-uncompressed")
    logger.info(f"extracting zip {zip_filename} -> {path}")
    with zipfile.ZipFile(zip_filename) as zfile:
        zfile.extractall(path=path)
    return path


def uncompress_tar_file(tar_filename: str, path: Optional[str] = None) -> str:
    head, tail = os.path.split(tar_filename)
    for ext in TAR_FILE_EXT:
        if tail.endswith(ext):
            tail = tail[: -(len(ext))]
            break

    path = path if path else os.path.join(head, f"{tail}-tar-uncompressed")
    logger.info(f"extracting tar {tar_filename} -> {path}")
    # NOTE: "r:*" mode opens both compressed (e.g ".tar.gz") and uncompressed ".tar" archives
    with tarfile.open(tar_filename, "r:*") as tfile:
        # NOTE(robinson): Mitigate against malicious content being extracted from the tar file.
        # This was added in Python 3.12
        # Ref: https://docs.python.org/3/library/tarfile.html#extraction-filters
        if sys.version_info >= (3, 12):
            tfile.extraction_filter = tarfile.tar_filter
        else:
            logger.warning(
                "Extraction filtering for tar files is available for Python 3.12 and above. "
                "Consider upgrading your Python version to improve security. "
                "See https://docs.python.org/3/library/tarfile.html#extraction-filters"
            )
        tfile.extractall(path=path)
    return path
