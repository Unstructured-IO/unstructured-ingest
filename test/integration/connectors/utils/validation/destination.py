import os
import shutil
from pathlib import Path

from test.integration.connectors.utils.validation.utils import ValidationConfig
from unstructured_ingest.utils.data_prep import get_data
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers, UploadStager


class StagerValidationConfigs(ValidationConfig):
    expected_count: int
    expected_folder: str = "stager"

    def stager_output_dir(self) -> Path:
        dir = self.test_output_dir() / self.expected_folder
        dir.mkdir(exist_ok=True, parents=True)
        return dir

    def stager_output_path(self, input_path: Path) -> Path:
        return self.stager_output_dir() / input_path.name


def run_all_stager_validations(
    configs: StagerValidationConfigs, input_file: Path, staged_filepath: Path
):
    # Validate matching extensions
    assert input_file.suffix == staged_filepath.suffix

    # Validate length
    staged_data = get_data(path=staged_filepath)
    assert len(staged_data) == configs.expected_count

    # Validate file
    expected_filepath = configs.stager_output_path(input_path=input_file)
    assert expected_filepath.exists(), f"{expected_filepath} does not exist"
    assert expected_filepath.is_file(), f"{expected_filepath} is not a file"
    if configs.detect_diff(expected_filepath=expected_filepath, current_filepath=staged_filepath):
        raise AssertionError(
            f"Current file ({staged_filepath}) does not match expected file: {expected_filepath}"
        )


def update_stager_fixtures(stager_output_path: Path, staged_filepath: Path):
    copied_filepath = stager_output_path / staged_filepath.name
    shutil.copy(staged_filepath, copied_filepath)


def stager_validation(
    stager: UploadStager,
    tmp_dir: Path,
    input_file: Path,
    configs: StagerValidationConfigs,
    overwrite_fixtures: bool = os.getenv("OVERWRITE_FIXTURES", "False").lower() == "true",
) -> None:
    # Run stager
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=input_file.name, filename=input_file.name),
        connector_type=configs.test_id,
        identifier="mock file data",
    )
    staged_filepath = stager.run(
        elements_filepath=input_file,
        file_data=file_data,
        output_dir=tmp_dir,
        output_filename=input_file.name,
    )
    if not overwrite_fixtures:
        print("Running validation")
        run_all_stager_validations(
            configs=configs, input_file=input_file, staged_filepath=staged_filepath
        )
    else:
        print("Running fixtures update")
        update_stager_fixtures(
            stager_output_path=configs.stager_output_dir(), staged_filepath=staged_filepath
        )
