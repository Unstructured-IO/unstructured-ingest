import filecmp
import shutil
from pathlib import Path
from typing import Callable, Optional

from pydantic import BaseModel

from test.integration.connectors.utils.constants import expected_results_path
from test.integration.connectors.utils.validation.equality import file_type_equality_check


class ValidationConfig(BaseModel):
    test_id: str
    file_equality_check: Optional[Callable[[Path, Path], bool]] = None

    def test_output_dir(self) -> Path:
        return expected_results_path / self.test_id

    def detect_diff(self, expected_filepath: Path, current_filepath: Path) -> bool:
        if expected_filepath.suffix != current_filepath.suffix:
            return True
        if file_equality_check := self.file_equality_check:
            return not file_equality_check(expected_filepath, current_filepath)
        current_suffix = expected_filepath.suffix
        if current_suffix in file_type_equality_check:
            equality_check_callable = file_type_equality_check[current_suffix]
            return not equality_check_callable(
                expected_filepath=expected_filepath, current_filepath=current_filepath
            )
        # Fallback is using filecmp.cmp to compare the files
        return not filecmp.cmp(expected_filepath, current_filepath, shallow=False)


def reset_dir(dir_path: Path) -> None:
    shutil.rmtree(path=dir_path, ignore_errors=True)
    dir_path.mkdir(parents=True)
