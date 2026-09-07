import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Optional

from unstructured_ingest.data_types.file_data import file_data_from_file
from unstructured_ingest.logger import logger
from unstructured_ingest.otel import OtelHandler
from unstructured_ingest.pipeline.interfaces import PipelineStep, iterable_input
from unstructured_ingest.processes.filter import Filterer

STEP_ID = "filter"


@dataclass
class FilterStep(PipelineStep):
    process: Filterer
    identifier: str = STEP_ID

    def __post_init__(self):
        config = self.process.config.model_dump_json() if self.process.config else None
        logger.info(f"created {self.identifier} with configs: {config}")

    def __call__(self, iterable: Optional[iterable_input] = None, stage: str = "indexed") -> Any:
        results = super().__call__(iterable=iterable)
        self.report_filter_counts(
            stage=stage,
            received=len(iterable or []),
            retained=len([r for r in (results or []) if r]),
        )
        return results

    def report_filter_counts(self, stage: str, received: int, retained: int) -> None:
        """Report how many records reached the configured filters and how many got through.

        This is the counterpart to the indexer's listing counts: it separates "the source had
        nothing" from "the source had things and file_glob/max_file_size rejected all of
        them". The pipeline runs the same Filterer up to three times (indexed, downloaded,
        uncompressed), so the stage is part of the key -- a single pair of keys would count a
        file that survived every pass once per pass.
        """
        OtelHandler.record_on_current_span(
            {
                f"filter.{stage}.received": received,
                f"filter.{stage}.retained": retained,
            }
        )
        if received and not retained:
            # Say what was observed, not why. A record also fails to be retained when
            # file_data_from_file raises on a malformed cached FileData and run_async
            # returns None, so naming the filter config as the cause would be wrong in
            # that case. An ERROR line precedes it there; the config stays as context.
            logger.warning(
                f"nothing to process after filtering {stage} content: none of the "
                f"{received} records survived this stage. Filter settings in effect: "
                f"{self.process.config.model_dump_json()}"
            )

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
