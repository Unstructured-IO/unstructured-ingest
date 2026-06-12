import json
import os
import re
import shutil
from pathlib import Path
from typing import Callable, Literal, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from deepdiff import DeepDiff
from pydantic import BaseModel, Field

from test.integration.connectors.utils.validation.utils import ValidationConfig
from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.interfaces import Downloader, Indexer

NONSTANDARD_METADATA_FIELDS = {
    "additional_metadata.@microsoft.graph.downloadUrl": [
        "additional_metadata",
        "@microsoft.graph.downloadUrl",
    ]
}

FILEDATA_CHECKS_TYPE = Callable[[FileData], None] | tuple[Callable[[FileData], None], ...]


class FixtureScrubber(BaseModel):
    """Sanitize a field in fixture output before persisting to disk.

    Applied at write time only (during ``OVERWRITE_FIXTURES=true`` runs);
    runtime connector behavior is unaffected. Use this for values that are
    sensitive (short-lived tokens, API keys) or noisy (rotating IDs) and that
    would otherwise be re-committed on every fixture regeneration.

    ``path`` uses the same dotted notation as ``exclude_fields`` and honors
    the same ``NONSTANDARD_METADATA_FIELDS`` escape hatch for keys whose
    names contain dots (e.g. ``@microsoft.graph.downloadUrl``).

    Pair with ``exclude_fields_extend``: scrubbers run at fixture-write time
    only, not at validation-read time. If a scrubber rewrites a value that
    also changes run-to-run (the usual reason to scrub), the on-disk fixture
    will diff against the live un-scrubbed value on the next test run unless
    the same path is also listed in ``exclude_fields_extend``. The bundled
    ``additional_metadata.@microsoft.graph.downloadUrl`` default is already
    excluded in the OneDrive/SharePoint tests.
    """

    path: str
    mode: Literal["drop", "redact", "strip_url_param"] = "drop"
    param: Optional[str] = None
    placeholder: str = "<redacted>"


# SharePoint / OneDrive Graph $batch responses bake a short-lived bearer
# token (`tempauth=v1.<jwt>`) into the per-item download URL. The token
# is bound to that one URL and expires quickly, but it's still a live
# credential in source control — strip it before committing fixtures.
DEFAULT_FIXTURE_SCRUBBERS: list[FixtureScrubber] = [
    FixtureScrubber(
        path="additional_metadata.@microsoft.graph.downloadUrl",
        mode="strip_url_param",
        param="tempauth",
    ),
]


class SourceValidationConfigs(ValidationConfig):
    expected_number_indexed_file_data: Optional[int] = None
    expected_num_files: Optional[int] = None
    predownload_file_data_check: Optional[FILEDATA_CHECKS_TYPE] = None
    postdownload_file_data_check: Optional[FILEDATA_CHECKS_TYPE] = None
    exclude_fields: list[str] = Field(
        default_factory=lambda: ["local_download_path", "metadata.date_processed"]
    )
    exclude_fields_extend: list[str] = Field(default_factory=list)
    fixture_scrubbers: list[FixtureScrubber] = Field(
        default_factory=lambda: list(DEFAULT_FIXTURE_SCRUBBERS)
    )
    fixture_scrubbers_extend: list[FixtureScrubber] = Field(default_factory=list)
    validate_downloaded_files: bool = False
    validate_file_data: bool = True

    def get_exclude_fields(self) -> list[str]:
        exclude_fields = self.exclude_fields
        exclude_fields.extend(self.exclude_fields_extend)
        return list(set(exclude_fields))

    def get_fixture_scrubbers(self) -> list[FixtureScrubber]:
        return [*self.fixture_scrubbers, *self.fixture_scrubbers_extend]

    def run_file_data_validation(
        self, predownload_file_data: FileData, postdownload_file_data: FileData
    ):
        if predownload_file_data_check := self.predownload_file_data_check:
            if isinstance(predownload_file_data_check, tuple):
                for check in predownload_file_data_check:
                    check(predownload_file_data)
            else:
                predownload_file_data_check(predownload_file_data)
        if postdownload_file_data_check := self.postdownload_file_data_check:
            if isinstance(postdownload_file_data_check, tuple):
                for check in postdownload_file_data_check:
                    check(postdownload_file_data)
            else:
                postdownload_file_data_check(postdownload_file_data)

    def run_download_dir_validation(self, download_dir: Path):
        if expected_num_files := self.expected_num_files:
            downloaded_files = [p for p in download_dir.rglob("*") if p.is_file()]
            assert len(downloaded_files) == expected_num_files

    def omit_ignored_fields(self, data: dict) -> dict:
        exclude_fields = self.get_exclude_fields()
        # Ignore fields that dynamically change every time the tests run
        copied_data = data.copy()

        for exclude_field in exclude_fields:
            exclude_field_vals = (
                NONSTANDARD_METADATA_FIELDS[exclude_field]
                if exclude_field in NONSTANDARD_METADATA_FIELDS
                else exclude_field.split(".")
            )
            if len(exclude_field_vals) == 1:
                current_val = copied_data
                drop_field = exclude_field_vals[0]
                copied_data.pop(exclude_field_vals[0], None)
            else:
                current_val = copied_data
                for val in exclude_field_vals[:-1]:
                    current_val = current_val.get(val, {})
                drop_field = exclude_field_vals[-1]
            if drop_field == "*":
                current_val.clear()
            else:
                current_val.pop(drop_field, None)
        return copied_data


def _resolve_field_parent(data: dict, path: str) -> tuple[Optional[dict], str]:
    """Walk ``data`` following the dotted ``path`` (honoring
    ``NONSTANDARD_METADATA_FIELDS``) and return the parent dict that holds
    the final key, along with that final key. Returns ``(None, "")`` if any
    intermediate segment is missing or not a dict, so callers can no-op
    cleanly on absent paths.
    """
    segments = (
        NONSTANDARD_METADATA_FIELDS[path]
        if path in NONSTANDARD_METADATA_FIELDS
        else path.split(".")
    )
    current = data
    for segment in segments[:-1]:
        nxt = current.get(segment)
        if not isinstance(nxt, dict):
            return None, ""
        current = nxt
    return current, segments[-1]


def apply_fixture_scrubbers(data: dict, scrubbers: list[FixtureScrubber]) -> None:
    """Apply each scrubber to ``data`` in place. Missing paths and type
    mismatches are silently skipped so per-test scrubber lists can be shared
    across connectors that don't emit every field.
    """
    for scrubber in scrubbers:
        parent, key = _resolve_field_parent(data, scrubber.path)
        if parent is None or key not in parent:
            continue
        if scrubber.mode == "drop":
            parent.pop(key, None)
        elif scrubber.mode == "redact":
            parent[key] = scrubber.placeholder
        elif scrubber.mode == "strip_url_param":
            value = parent[key]
            if not isinstance(value, str) or not scrubber.param:
                continue
            parts = urlsplit(value)
            new_query = urlencode(
                [
                    (k, v)
                    for k, v in parse_qsl(parts.query, keep_blank_values=True)
                    if k != scrubber.param
                ]
            )
            parent[key] = urlunsplit(
                (parts.scheme, parts.netloc, parts.path, new_query, parts.fragment)
            )


# FsspecDownloader writes each file into a fresh tempfile.mkdtemp("unstructured_") subdir
# to avoid path collisions. Strip that segment so fixtures capture the logical structure
# rather than a randomized suffix that changes every run.
_FSSPEC_TEMP_DIR_PATTERN = re.compile(r"^unstructured_[a-zA-Z0-9_-]+/")


def get_files(dir_path: Path) -> list[str]:
    return [
        _FSSPEC_TEMP_DIR_PATTERN.sub("", str(f).replace(str(dir_path), "").lstrip("/"))
        for f in dir_path.rglob("*")
        if f.is_file()
    ]


def check_files(expected_output_dir: Path, all_file_data: list[FileData]):
    expected_files = get_files(dir_path=expected_output_dir)
    current_files = [f"{file_data.identifier}.json" for file_data in all_file_data]
    diff = set(expected_files) ^ set(current_files)
    assert not diff, "diff in files that exist: {}".format(", ".join(diff))


def check_files_in_paths(expected_output_dir: Path, current_output_dir: Path):
    expected_files = get_files(dir_path=expected_output_dir)
    current_files = get_files(dir_path=current_output_dir)
    diff = set(expected_files) ^ set(current_files)
    assert not diff, "diff in files that exist: {}".format(", ".join(diff))


def check_contents(
    expected_output_dir: Path, all_file_data: list[FileData], configs: SourceValidationConfigs
):
    found_diff = False
    for file_data in all_file_data:
        file_data_path = expected_output_dir / f"{file_data.identifier}.json"
        with file_data_path.open("r") as file:
            expected_file_data_contents = json.load(file)
        current_file_data_contents = json.loads(file_data.model_dump_json())
        expected_file_data_contents = configs.omit_ignored_fields(expected_file_data_contents)
        current_file_data_contents = configs.omit_ignored_fields(current_file_data_contents)
        diff = DeepDiff(expected_file_data_contents, current_file_data_contents)
        if diff:
            found_diff = True
            print(diff.to_json(indent=2))
    assert not found_diff, f"Diffs found between files: {found_diff}"


def check_raw_file_contents(
    expected_output_dir: Path,
    current_output_dir: Path,
    configs: SourceValidationConfigs,
):
    found_diff = False
    files = []
    for current_file_path in current_output_dir.rglob("*"):
        if not current_file_path.is_file():
            continue
        relative = str(current_file_path.relative_to(current_output_dir))
        # Strip the unstructured_<random>/ tempdir segment when locating the
        # corresponding fixture; the on-disk file still lives under the random
        # subdir so don't strip it from current_file_path.
        expected_relative = _FSSPEC_TEMP_DIR_PATTERN.sub("", relative)
        expected_file_path = expected_output_dir / expected_relative
        if configs.detect_diff(expected_file_path, current_file_path):
            found_diff = True
            files.append(str(expected_file_path))
            print(f"diffs between files {expected_file_path} and {current_file_path}")
    assert not found_diff, "Diffs found between files: {}".format(", ".join(files))


def run_expected_results_validation(
    expected_output_dir: Path, all_file_data: list[FileData], configs: SourceValidationConfigs
):
    check_files(expected_output_dir=expected_output_dir, all_file_data=all_file_data)
    check_contents(
        expected_output_dir=expected_output_dir, all_file_data=all_file_data, configs=configs
    )


def run_expected_download_files_validation(
    expected_output_dir: Path,
    current_download_dir: Path,
    configs: SourceValidationConfigs,
):
    check_files_in_paths(
        expected_output_dir=expected_output_dir, current_output_dir=current_download_dir
    )
    check_raw_file_contents(
        expected_output_dir=expected_output_dir,
        current_output_dir=current_download_dir,
        configs=configs,
    )


def run_directory_structure_validation(expected_output_dir: Path, download_files: list[str]):
    s3_keys_file = expected_output_dir / "expected_s3_keys.json"

    if s3_keys_file.exists():
        with s3_keys_file.open("r") as f:
            s3_keys = json.load(f)["s3_keys"]

        expected_filenames = {Path(s3_key).name for s3_key in s3_keys}
        actual_filenames = {Path(download_file).name for download_file in download_files}

        assert expected_filenames == actual_filenames, (
            f"Expected filenames: {sorted(expected_filenames)}, "
            f"Got filenames: {sorted(actual_filenames)}"
        )
    else:
        directory_record = expected_output_dir / "directory_structure.json"
        with directory_record.open("r") as f:
            directory_structure = json.load(f)["directory_structure"]
        assert directory_structure == download_files


def update_fixtures(
    output_dir: Path,
    download_dir: Path,
    all_file_data: list[FileData],
    save_downloads: bool = False,
    save_filedata: bool = True,
    fixture_scrubbers: Optional[list[FixtureScrubber]] = None,
):
    # Rewrite the current file data
    if not output_dir.exists():
        output_dir.mkdir(parents=True)
    if save_filedata:
        file_data_output_path = output_dir / "file_data"
        shutil.rmtree(path=file_data_output_path, ignore_errors=True)
        print(
            f"Writing {len(all_file_data)} file data to "
            f"saved fixture location {file_data_output_path}"
        )
        file_data_output_path.mkdir(parents=True, exist_ok=True)
        for file_data in all_file_data:
            file_data_path = file_data_output_path / f"{file_data.identifier}.json"
            payload = json.loads(file_data.model_dump_json())
            if fixture_scrubbers:
                apply_fixture_scrubbers(payload, fixture_scrubbers)
            with file_data_path.open(mode="w") as f:
                json.dump(payload, f, indent=2)
                f.write("\n")

    # Record file structure of download directory
    download_files = get_files(dir_path=download_dir)
    download_files.sort()
    download_dir_record = output_dir / "directory_structure.json"
    with download_dir_record.open(mode="w") as f:
        json.dump({"directory_structure": download_files}, f, indent=2)

    # If applicable, save raw downloads
    if save_downloads:
        raw_download_output_path = output_dir / "downloads"
        shutil.rmtree(path=raw_download_output_path, ignore_errors=True)
        print(
            f"Writing {len(download_files)} downloaded files to "
            f"saved fixture location {raw_download_output_path}"
        )
        shutil.copytree(download_dir, raw_download_output_path)


def run_all_validations(
    configs: SourceValidationConfigs,
    predownload_file_data: list[FileData],
    postdownload_file_data: list[FileData],
    download_dir: Path,
    test_output_dir: Path,
):
    if expected_number_indexed_file_data := configs.expected_number_indexed_file_data:
        assert len(predownload_file_data) == expected_number_indexed_file_data, (
            f"expected {expected_number_indexed_file_data} but got {len(predownload_file_data)}"
        )
    if expected_num_files := configs.expected_num_files:
        assert len(postdownload_file_data) == expected_num_files, (
            f"expected {expected_num_files} but got {len(postdownload_file_data)}"
        )

    for pre_data, post_data in zip(predownload_file_data, postdownload_file_data):
        configs.run_file_data_validation(
            predownload_file_data=pre_data, postdownload_file_data=post_data
        )
    configs.run_download_dir_validation(download_dir=download_dir)
    if configs.validate_file_data:
        run_expected_results_validation(
            expected_output_dir=test_output_dir / "file_data",
            all_file_data=get_all_file_data(
                all_predownload_file_data=predownload_file_data,
                all_postdownload_file_data=postdownload_file_data,
            ),
            configs=configs,
        )
    download_files = get_files(dir_path=download_dir)
    download_files.sort()
    run_directory_structure_validation(
        expected_output_dir=configs.test_output_dir(), download_files=download_files
    )
    if configs.validate_downloaded_files:
        run_expected_download_files_validation(
            expected_output_dir=test_output_dir / "downloads",
            current_download_dir=download_dir,
            configs=configs,
        )


def get_all_file_data(
    all_postdownload_file_data: list[FileData], all_predownload_file_data: list[FileData]
) -> list[FileData]:
    all_file_data = all_postdownload_file_data
    indexed_file_data = [
        fd
        for fd in all_predownload_file_data
        if fd.identifier not in [f.identifier for f in all_file_data]
    ]
    all_file_data += indexed_file_data
    return all_file_data


async def source_connector_validation(
    indexer: Indexer,
    downloader: Downloader,
    configs: SourceValidationConfigs,
    overwrite_fixtures: bool = os.getenv("OVERWRITE_FIXTURES", "False").lower() == "true",
) -> None:
    # Run common validations on the process of running a source connector, supporting dynamic
    # validators that get passed in along with comparisons on the saved expected values.
    # If overwrite_fixtures is st to True, will ignore all validators but instead overwrite the
    # expected values with what gets generated by this test.
    all_predownload_file_data = []
    all_postdownload_file_data = []
    indexer.precheck()
    download_dir = downloader.download_config.download_dir
    test_output_dir = configs.test_output_dir()
    if indexer.is_async():
        async for file_data in indexer.run_async():
            assert file_data
            predownload_file_data = file_data.model_copy(deep=True)
            all_predownload_file_data.append(predownload_file_data)
            if downloader.is_async():
                resp = await downloader.run_async(file_data=file_data)
            else:
                resp = downloader.run(file_data=file_data)
            if isinstance(resp, list):
                for r in resp:
                    postdownload_file_data = r["file_data"].model_copy(deep=True)
                    all_postdownload_file_data.append(postdownload_file_data)
            else:
                postdownload_file_data = resp["file_data"].model_copy(deep=True)
                all_postdownload_file_data.append(postdownload_file_data)
    else:
        for file_data in indexer.run():
            assert file_data
            predownload_file_data = file_data.model_copy(deep=True)
            all_predownload_file_data.append(predownload_file_data)
            if downloader.is_async():
                resp = await downloader.run_async(file_data=file_data)
            else:
                resp = downloader.run(file_data=file_data)
            if isinstance(resp, list):
                for r in resp:
                    postdownload_file_data = r["file_data"].model_copy(deep=True)
                    all_postdownload_file_data.append(postdownload_file_data)
            else:
                postdownload_file_data = resp["file_data"].model_copy(deep=True)
                all_postdownload_file_data.append(postdownload_file_data)
    if not overwrite_fixtures:
        print("Running validation")
        run_all_validations(
            configs=configs,
            predownload_file_data=all_predownload_file_data,
            postdownload_file_data=all_postdownload_file_data,
            download_dir=download_dir,
            test_output_dir=test_output_dir,
        )
    else:
        print("Running fixtures update")
        update_fixtures(
            output_dir=test_output_dir,
            download_dir=download_dir,
            all_file_data=get_all_file_data(
                all_predownload_file_data=all_predownload_file_data,
                all_postdownload_file_data=all_postdownload_file_data,
            ),
            save_downloads=configs.validate_downloaded_files,
            save_filedata=configs.validate_file_data,
            fixture_scrubbers=configs.get_fixture_scrubbers(),
        )


def source_filedata_display_name_set_check(file_data: FileData) -> None:
    """
    Check if the display_name of the file_data is set.
    If not, raise an AssertionError.
    """
    assert file_data.display_name, (
        "FileData display_name is not set. This is required for a given connector."
    )
