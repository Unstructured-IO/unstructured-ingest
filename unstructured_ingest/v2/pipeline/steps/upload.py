import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, TypedDict

from unstructured_ingest.v2.interfaces.file_data import file_data_from_file
from unstructured_ingest.v2.interfaces.uploader import UploadContent
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.pipeline.interfaces import BatchPipelineStep
from unstructured_ingest.v2.pipeline.otel import instrument

STEP_ID = "upload"


class UploadStepContent(TypedDict):
    path: str
    file_data_path: str


@dataclass
class UploadStep(BatchPipelineStep):
    identifier: str = STEP_ID

    def __str__(self):
        return f"{self.identifier} ({self.process.__class__.__name__})"

    def __post_init__(self):
        config = (
            self.process.upload_config.model_dump_json() if self.process.upload_config else None
        )
        connection_config = (
            self.process.connection_config.model_dump_json()
            if self.process.connection_config
            else None
        )
        logger.info(
            f"Created {self.identifier} with configs: {config}, "
            f"connection configs: {connection_config}"
        )

    @instrument(span_name=STEP_ID)
    def _run_batch(self, contents: list[UploadStepContent]) -> None:
        upload_contents = [
            UploadContent(path=Path(c["path"]), file_data=file_data_from_file(c["file_data_path"]))
            for c in contents
        ]
        self.process.run_batch(contents=upload_contents)

    async def _run_async(self, path: str, file_data_path: str, fn: Optional[Callable] = None):
        fn = fn or self.process.run_async
        fn_kwargs = {"path": Path(path), "file_data": file_data_from_file(path=file_data_path)}
        if not asyncio.iscoroutinefunction(fn):
            fn(**fn_kwargs)
        elif semaphore := self.context.semaphore:
            async with semaphore:
                await fn(**fn_kwargs)
        else:
            await fn(**fn_kwargs)
