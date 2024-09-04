import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, TypedDict

from unstructured_ingest.v2.interfaces.file_data import FileData
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.pipeline.interfaces import PipelineStep
from unstructured_ingest.v2.processes.uncompress import Uncompressor

STEP_ID = "uncompress"


class UncompressStepResponse(TypedDict):
    file_data_path: str
    path: str


@dataclass
class UncompressStep(PipelineStep):
    process: Uncompressor
    identifier: str = STEP_ID

    def __post_init__(self):
        config = self.process.config.json() if self.process.config else None
        logger.info(f"Created {self.identifier} with configs: {config}")

    async def _run_async(
        self, fn: Callable, path: str, file_data_path: str
    ) -> list[UncompressStepResponse]:
        file_data = FileData.from_file(path=file_data_path)
        fn_kwargs = {"file_data": file_data}
        if not asyncio.iscoroutinefunction(fn):
            new_file_data = fn(**fn_kwargs)
        elif semaphore := self.context.semaphore:
            async with semaphore:
                new_file_data = await fn(**fn_kwargs)
        else:
            new_file_data = await fn(**fn_kwargs)
        responses = []
        for new_file in new_file_data:
            new_file_data_path = Path(file_data_path).parent / f"{new_file.identifier}.json"
            new_file.to_file(path=str(new_file_data_path.resolve()))
            responses.append(
                UncompressStepResponse(
                    path=new_file.local_download_path,
                    file_data_path=str(new_file_data_path),
                )
            )
        return responses
