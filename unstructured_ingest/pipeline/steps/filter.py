import asyncio
from dataclasses import dataclass
from typing import Callable, Optional

from unstructured_ingest.data_types.file_data import file_data_from_file
from unstructured_ingest.logger import logger
from unstructured_ingest.pipeline.interfaces import PipelineStep
from unstructured_ingest.processes.filter import Filterer

STEP_ID = "filter"


@dataclass
class FilterStep(PipelineStep):
    process: Filterer
    identifier: str = STEP_ID

    def __post_init__(self):
        config = self.process.config.model_dump_json() if self.process.config else None
        logger.info(f"created {self.identifier} with configs: {config}")

    async def _run_async(self, fn: Callable, file_data_path: str, **kwargs) -> Optional[dict]:
        file_data = file_data_from_file(path=file_data_path)
        fn_kwargs = {"file_data": file_data}
        if not asyncio.iscoroutinefunction(fn):
            resp = fn(**fn_kwargs)
        elif semaphore := self.context.semaphore:
            async with semaphore:
                resp = await fn(**fn_kwargs)
        else:
            resp = await fn(**fn_kwargs)

        if resp:
            return {"file_data_path": file_data_path}
        return None
