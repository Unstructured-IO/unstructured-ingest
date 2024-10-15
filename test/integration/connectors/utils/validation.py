import json
import os
import shutil
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Optional

from deepdiff import DeepDiff

from test.integration.connectors.utils.constants import expected_results_path
from unstructured_ingest.v2.interfaces import Downloader, FileData, Indexer


@dataclass
class ValidationConfigs:
    test_id: str
    expected_num_files: Optional[int] = None
    predownload_filedata_check: Optional[Callable[[FileData], None]] = None
    postdownload_filedata_check: Optional[Callable[[FileData], None]] = None

    def run_file_data_validation(
        self, predownload_filedata: FileData, postdownload_file_data: FileData
    ):
        if predownload_filedata_check := self.predownload_filedata_check:
            predownload_filedata_check(predownload_filedata)
        if postdownload_filedata_check := self.postdownload_filedata_check:
            postdownload_filedata_check(postdownload_file_data)

    def run_download_dir_validation(self, download_dir: Path):
        if expected_num_files := self.expected_num_files:
            downloaded_files = [p for p in download_dir.rglob("*") if p.is_file()]
            assert len(downloaded_files) == expected_num_files

    def test_output_dir(self) -> Path:
        return expected_results_path / self.test_id


def get_files(dir_path: Path) -> list[str]:
    return [
        str(f).replace(str(dir_path), "").lstrip("/") for f in dir_path.iterdir() if f.is_file()
    ]


def check_files(expected_output_dir: Path, all_file_data: list[FileData]):
    expected_files = get_files(dir_path=expected_output_dir)
    current_files = [f"{file_data.identifier}.json" for file_data in all_file_data]
    diff = set(expected_files) ^ set(current_files)
    assert not diff, "diff in files that exist: {}".format(", ".join(diff))


def omit_ignored_fields(data: dict) -> None:
    # Ignore fields that dynamically change every time the tests run
    data.pop("local_download_path", None)
    if metadata := data.get("metadata"):
        metadata.pop("date_processed", None)


def check_contents(expected_output_dir: Path, all_file_data: list[FileData]):
    found_diff = False
    for file_data in all_file_data:
        file_data_path = expected_output_dir / f"{file_data.identifier}.json"
        with file_data_path.open("r") as file:
            expected_file_data_contents = json.load(file)
        current_file_data_contents = file_data.to_dict()
        omit_ignored_fields(expected_file_data_contents)
        omit_ignored_fields(current_file_data_contents)
        diff = DeepDiff(expected_file_data_contents, current_file_data_contents)
        if diff:
            found_diff = True
            print(diff.to_json(indent=2))
    assert not found_diff, "Diffs found between files"


def run_expected_results_validation(expected_output_dir: Path, all_file_data: list[FileData]):
    check_files(expected_output_dir=expected_output_dir, all_file_data=all_file_data)
    check_contents(expected_output_dir=expected_output_dir, all_file_data=all_file_data)


def run_directory_structure_validation(expected_output_dir: Path, download_files: list[str]):
    directory_record = expected_output_dir / "directory_structure.json"
    with directory_record.open("r") as directory_file:
        directory_file_contents = json.load(directory_file)
    directory_structure = directory_file_contents["directory_structure"]
    assert directory_structure == download_files


async def source_connector_validation(
    indexer: Indexer,
    downloader: Downloader,
    configs: ValidationConfigs,
    overwrite_fixtures: bool = os.getenv("OVERWRITE_FIXTURES", "False").lower() == "true",
) -> None:
    persistent_file_data = []
    for file_data in indexer.run():
        assert file_data
        predownload_filedata = replace(file_data)
        if downloader.is_async():
            resp = await downloader.run_async(file_data=file_data)
        else:
            resp = downloader.run(file_data=file_data)
        postdownload_file_data = replace(resp["file_data"])
        if not overwrite_fixtures:
            configs.run_file_data_validation(
                predownload_filedata=predownload_filedata,
                postdownload_file_data=postdownload_file_data,
            )
        persistent_file_data.append(postdownload_file_data)
    if not overwrite_fixtures:
        configs.run_download_dir_validation(download_dir=downloader.download_config.download_dir)
        run_expected_results_validation(
            expected_output_dir=configs.test_output_dir() / "file_data",
            all_file_data=persistent_file_data,
        )
        download_files = get_files(dir_path=downloader.download_config.download_dir)
        download_files.sort()
        run_directory_structure_validation(
            expected_output_dir=configs.test_output_dir(), download_files=download_files
        )
    else:
        # Delete current files
        output_dir = configs.test_output_dir()
        shutil.rmtree(path=output_dir, ignore_errors=True)
        output_dir.mkdir(parents=True)
        # Rewrite the current file data
        file_data_output_path = output_dir / "file_data"
        file_data_output_path.mkdir(parents=True)
        for file_data in persistent_file_data:
            file_data_path = file_data_output_path / f"{file_data.identifier}.json"
            with file_data_path.open(mode="w") as f:
                json.dump(file_data.to_dict(), f, indent=2)

        # Record file structure of download directory
        download_files = get_files(dir_path=downloader.download_config.download_dir)
        download_files.sort()
        download_dir_record = output_dir / "directory_structure.json"
        with download_dir_record.open(mode="w") as f:
            json.dump({"directory_structure": download_files}, f, indent=2)
