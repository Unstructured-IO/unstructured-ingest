import filecmp
import json
import os
import shutil
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from deepdiff import DeepDiff

from test.integration.connectors.utils.constants import expected_results_path
from unstructured_ingest.v2.interfaces import Downloader, FileData, Indexer


def pandas_df_equality_check(expected_filepath: Path, current_filepath: Path) -> bool:
    expected_df = pd.read_csv(expected_filepath)
    current_df = pd.read_csv(current_filepath)
    if expected_df.equals(current_df):
        return True
    # Print diff
    diff = expected_df.merge(current_df, indicator=True, how="left").loc[
        lambda x: x["_merge"] != "both"
    ]
    print("diff between expected and current df:")
    print(diff)
    return False


@dataclass
class ValidationConfigs:
    test_id: str
    expected_number_indexed_file_data: Optional[int] = None
    expected_num_files: Optional[int] = None
    predownload_file_data_check: Optional[Callable[[FileData], None]] = None
    postdownload_file_data_check: Optional[Callable[[FileData], None]] = None
    exclude_fields: list[str] = field(
        default_factory=lambda: ["local_download_path", "metadata.date_processed"]
    )
    exclude_fields_extend: list[str] = field(default_factory=list)
    validate_downloaded_files: bool = False
    downloaded_file_equality_check: Optional[Callable[[Path, Path], bool]] = None

    def get_exclude_fields(self) -> list[str]:
        exclude_fields = self.exclude_fields
        exclude_fields.extend(self.exclude_fields_extend)
        return exclude_fields

    def run_file_data_validation(
        self, predownload_file_data: FileData, postdownload_file_data: FileData
    ):
        if predownload_file_data_check := self.predownload_file_data_check:
            predownload_file_data_check(predownload_file_data)
        if postdownload_file_data_check := self.postdownload_file_data_check:
            postdownload_file_data_check(postdownload_file_data)

    def run_download_dir_validation(self, download_dir: Path):
        if expected_num_files := self.expected_num_files:
            downloaded_files = [p for p in download_dir.rglob("*") if p.is_file()]
            assert len(downloaded_files) == expected_num_files

    def test_output_dir(self) -> Path:
        return expected_results_path / self.test_id

    def omit_ignored_fields(self, data: dict) -> dict:
        exclude_fields = self.get_exclude_fields()
        # Ignore fields that dynamically change every time the tests run
        copied_data = data.copy()
        for exclude_field in exclude_fields:
            exclude_field_vals = exclude_field.split(".")
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


def get_files(dir_path: Path) -> list[str]:
    return [
        str(f).replace(str(dir_path), "").lstrip("/") for f in dir_path.iterdir() if f.is_file()
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
    expected_output_dir: Path, all_file_data: list[FileData], configs: ValidationConfigs
):
    found_diff = False
    for file_data in all_file_data:
        file_data_path = expected_output_dir / f"{file_data.identifier}.json"
        with file_data_path.open("r") as file:
            expected_file_data_contents = json.load(file)
        current_file_data_contents = file_data.to_dict()
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
    configs: ValidationConfigs,
):
    current_files = get_files(dir_path=current_output_dir)
    found_diff = False
    files = []
    for current_file in current_files:
        current_file_path = current_output_dir / current_file
        expected_file_path = expected_output_dir / current_file
        if downloaded_file_equality_check := configs.downloaded_file_equality_check:
            is_different = downloaded_file_equality_check(expected_file_path, current_file_path)
        elif expected_file_path.suffix == ".csv" and current_file_path.suffix == ".csv":
            is_different = not pandas_df_equality_check(
                expected_filepath=expected_file_path, current_filepath=current_file_path
            )
        else:
            is_different = not filecmp.cmp(expected_file_path, current_file_path, shallow=False)
        if is_different:
            found_diff = True
            files.append(str(expected_file_path))
            print(f"diffs between files {expected_file_path} and {current_file_path}")
    assert not found_diff, "Diffs found between files: {}".format(", ".join(files))


def run_expected_results_validation(
    expected_output_dir: Path, all_file_data: list[FileData], configs: ValidationConfigs
):
    check_files(expected_output_dir=expected_output_dir, all_file_data=all_file_data)
    check_contents(
        expected_output_dir=expected_output_dir, all_file_data=all_file_data, configs=configs
    )


def run_expected_download_files_validation(
    expected_output_dir: Path,
    current_download_dir: Path,
    configs: ValidationConfigs,
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
    directory_record = expected_output_dir / "directory_structure.json"
    with directory_record.open("r") as directory_file:
        directory_file_contents = json.load(directory_file)
    directory_structure = directory_file_contents["directory_structure"]
    assert directory_structure == download_files


def update_fixtures(
    output_dir: Path,
    download_dir: Path,
    all_file_data: list[FileData],
    save_downloads: bool = False,
):
    # Delete current files
    shutil.rmtree(path=output_dir, ignore_errors=True)
    output_dir.mkdir(parents=True)
    # Rewrite the current file data
    file_data_output_path = output_dir / "file_data"
    file_data_output_path.mkdir(parents=True, exist_ok=True)
    for file_data in all_file_data:
        file_data_path = file_data_output_path / f"{file_data.identifier}.json"
        with file_data_path.open(mode="w") as f:
            json.dump(file_data.to_dict(), f, indent=2)

    # Record file structure of download directory
    download_files = get_files(dir_path=download_dir)
    download_files.sort()
    download_dir_record = output_dir / "directory_structure.json"
    with download_dir_record.open(mode="w") as f:
        json.dump({"directory_structure": download_files}, f, indent=2)

    # If applicable, save raw downloads
    if save_downloads:
        raw_download_output_path = output_dir / "downloads"
        shutil.copytree(download_dir, raw_download_output_path)


def run_all_validations(
    configs: ValidationConfigs,
    predownload_file_data: list[FileData],
    postdownload_file_data: list[FileData],
    download_dir: Path,
    test_output_dir: Path,
):
    if expected_number_indexed_file_data := configs.expected_number_indexed_file_data:
        assert (
            len(predownload_file_data) == expected_number_indexed_file_data
        ), f"expected {expected_number_indexed_file_data} but got {len(predownload_file_data)}"
    if expected_num_files := configs.expected_num_files:
        assert len(postdownload_file_data) == expected_num_files

    for pre_data, post_data in zip(predownload_file_data, postdownload_file_data):
        configs.run_file_data_validation(
            predownload_file_data=pre_data, postdownload_file_data=post_data
        )
    configs.run_download_dir_validation(download_dir=download_dir)
    run_expected_results_validation(
        expected_output_dir=test_output_dir / "file_data",
        all_file_data=postdownload_file_data,
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


async def source_connector_validation(
    indexer: Indexer,
    downloader: Downloader,
    configs: ValidationConfigs,
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
    for file_data in indexer.run():
        assert file_data
        predownload_file_data = replace(file_data)
        all_predownload_file_data.append(predownload_file_data)
        if downloader.is_async():
            resp = await downloader.run_async(file_data=file_data)
        else:
            resp = downloader.run(file_data=file_data)
        if isinstance(resp, list):
            for r in resp:
                postdownload_file_data = replace(r["file_data"])
                all_postdownload_file_data.append(postdownload_file_data)
        else:
            postdownload_file_data = replace(resp["file_data"])
            all_postdownload_file_data.append(postdownload_file_data)
    if not overwrite_fixtures:
        run_all_validations(
            configs=configs,
            predownload_file_data=all_predownload_file_data,
            postdownload_file_data=all_postdownload_file_data,
            download_dir=download_dir,
            test_output_dir=test_output_dir,
        )
    else:
        update_fixtures(
            output_dir=test_output_dir,
            download_dir=download_dir,
            all_file_data=all_postdownload_file_data,
            save_downloads=configs.validate_downloaded_files,
        )
