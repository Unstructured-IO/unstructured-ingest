#!/usr/bin/env python3

import json
import shutil
from pathlib import Path

import click
from deepdiff import DeepDiff

scripts_dir = Path(__file__).parent
base_expected_output_dir = scripts_dir / "expected-structured-output"
base_current_output_dir = scripts_dir / "structured-output"


class CheckError(Exception):
    pass


def get_files(dir_path: Path) -> list[str]:
    return [
        str(f).replace(str(dir_path), "").lstrip("/") for f in dir_path.iterdir() if f.is_file()
    ]


def check_files(expected_output_dir: Path, current_output_dir: Path):
    expected_files = get_files(dir_path=expected_output_dir)
    current_files = get_files(dir_path=current_output_dir)
    diff = set(expected_files) ^ set(current_files)
    if diff:
        print("diff in files that exist: {}".format(", ".join(diff)))
        print('to update test fixtures, "export OVERWRITE_FIXTURES=true" and rerun this script')
        raise CheckError("The same files don't exist in both locations")


def check_contents(expected_output_dir: Path, current_output_dir: Path):
    current_files = get_files(dir_path=current_output_dir)
    found_diff = False
    for current_file in current_files:
        current_file_path = current_output_dir / current_file
        expected_file_path = expected_output_dir / current_file
        diffs = compare_files(expected_file=expected_file_path, current_file=current_file_path)
        if diffs:
            found_diff = True
            print(f"diffs between files {expected_file_path} and {current_file_path}")
            print('to update test fixtures, "export OVERWRITE_FIXTURES=true" and rerun this script')
            for diff in diffs:
                print(diff.to_json(indent=2))
    if found_diff:
        raise CheckError("Diffs found between files")


def print_diffs(expected: list[dict], current: list[dict]):
    expected = dict(enumerate(expected))
    current = dict(enumerate(current))
    only_in_expected = {
        i: e
        for i, e in expected.items()
        if e["element_id"] not in [c["element_id"] for c in current.values()]
    }
    only_in_current = {
        i: c
        for i, c in current.items()
        if c["element_id"] not in [e["element_id"] for e in expected.values()]
    }
    if only_in_expected:
        print("Elements only in expected:")
        print(json.dumps(only_in_expected, indent=2))
    if only_in_current:
        print("Elements only in current:")
        print(json.dumps(only_in_current, indent=2))


def omit_ignored_fields(data: dict) -> None:
    # Ignore text and the dependant fields since these can change based on
    # partitioning which isn't derived from this repo
    data.pop("text", None)
    data.pop("element_id", None)
    if metadata := data.get("metadata"):
        metadata.pop("text_as_html", None)
        metadata.pop("languages", None)


def compare_files(expected_file: Path, current_file: Path) -> list[DeepDiff]:
    print(f"Comparing content of {expected_file} to {current_file}")
    with expected_file.open("r") as expected:
        expected_data = json.load(expected)
    with current_file.open("r") as current:
        current_data = json.load(current)
    if len(current_data) != len(expected_data):
        print_diffs(expected=expected_data, current=current_data)
        raise CheckError(
            f"The number of elements of current ({len(current_data)}) "
            f"and expected ({len(expected_data)}) files don't match"
        )
    diffs = []
    for expected_element, current_element in zip(expected_data, current_data):
        omit_ignored_fields(data=expected_element)
        omit_ignored_fields(data=current_element)

        diff = DeepDiff(expected_element, current_element)
        if diff:
            diffs.append(diff)

    return diffs


def check_dir(dir_path: Path):
    if not dir_path.exists():
        raise ValueError(f"Expected directory {dir_path} does not exist")
    if not dir_path.is_dir():
        raise ValueError(f"Expected directory {dir_path} is not a directory")


def overwrite_outputs(expected_output_dir: Path, current_output_dir: Path):
    print(f"Overwriting output in {expected_output_dir} with content from {current_output_dir}")
    # delete old fixture
    shutil.rmtree(expected_output_dir)

    # copy over new content
    shutil.copytree(current_output_dir, expected_output_dir)


def run_checks(expected_output_dir: Path, current_output_dir: Path):
    print(f"Checking structured output between {expected_output_dir} and {current_output_dir}")
    check_files(expected_output_dir=expected_output_dir, current_output_dir=current_output_dir)
    check_contents(expected_output_dir=expected_output_dir, current_output_dir=current_output_dir)
    print("All tests passed!")


@click.command()
@click.option("--output-folder-name", required=True, type=str)
@click.option("--overwrite-fixtures", type=bool, default=False, envvar="OVERWRITE_FIXTURES")
def check_outputs(output_folder_name: str, overwrite_fixtures: bool):

    expected_output_dir = base_expected_output_dir / output_folder_name
    current_output_dir = base_current_output_dir / output_folder_name
    check_dir(dir_path=expected_output_dir)
    check_dir(dir_path=current_output_dir)
    try:
        run_checks(expected_output_dir=expected_output_dir, current_output_dir=current_output_dir)
    except CheckError as e:
        if overwrite_fixtures:
            overwrite_outputs(
                expected_output_dir=expected_output_dir, current_output_dir=current_output_dir
            )
        else:
            raise e


if __name__ == "__main__":
    check_outputs()
