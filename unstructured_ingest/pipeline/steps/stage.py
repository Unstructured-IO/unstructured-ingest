import asyncio
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, TypedDict

from unstructured_ingest.data_types.file_data import file_data_from_file
from unstructured_ingest.interfaces import UploadStager
from unstructured_ingest.logger import logger
from unstructured_ingest.pipeline.interfaces import PipelineStep
from unstructured_ingest.utils.pydantic_models import serialize_base_model_json

STEP_ID = "upload_stage"


class UploadStageStepResponse(TypedDict):
    file_data_path: str
    path: str


@dataclass
class UploadStageStep(PipelineStep):
    process: UploadStager
    identifier: str = STEP_ID

    def __str__(self):
        return f"{self.identifier} ({self.process.__class__.__name__})"

    def __post_init__(self):
        config = (
            self.process.upload_stager_config.model_dump_json()
            if self.process.upload_stager_config
            else None
        )
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"created {self.identifier} with configs: {config}")

    async def _run_async(
        self, fn: Callable, path: str, file_data_path: str
    ) -> UploadStageStepResponse:
        path = Path(path)
        # Maintain extension
        output_filename = f"{self.get_hash(extras=[path.name])}{path.suffix}"
        fn_kwargs = {
            "elements_filepath": path,
            "file_data": file_data_from_file(path=file_data_path),
            "output_dir": self.cache_dir,
            "output_filename": output_filename,
        }
        if not asyncio.iscoroutinefunction(fn):
            staged_output_path = fn(**fn_kwargs)
        elif semaphore := self.context.semaphore:
            async with semaphore:
                staged_output_path = await fn(**fn_kwargs)
        else:
            staged_output_path = await fn(**fn_kwargs)
        return UploadStageStepResponse(file_data_path=file_data_path, path=str(staged_output_path))

    def get_hash(self, extras: Optional[list[str]]) -> str:
        hashable_string = serialize_base_model_json(
            model=self.process.upload_stager_config, sort_keys=True, ensure_ascii=True
        )
        if extras:
            hashable_string += "".join(extras)
        return hashlib.sha256(hashable_string.encode()).hexdigest()[:12]
