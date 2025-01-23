import asyncio
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, TypedDict

from unstructured_ingest.utils.data_prep import write_data
from unstructured_ingest.v2.interfaces import FileData
from unstructured_ingest.v2.interfaces.file_data import file_data_from_file
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.pipeline.interfaces import PipelineStep
from unstructured_ingest.v2.processes.partitioner import Partitioner
from unstructured_ingest.v2.utils import serialize_base_model_json

STEP_ID = "partition"


class PartitionStepResponse(TypedDict):
    file_data_path: str
    path: str


@dataclass
class PartitionStep(PipelineStep):
    process: Partitioner
    identifier: str = STEP_ID

    def __str__(self):
        return f"{self.identifier} ({self.process.config.strategy})"

    def __post_init__(self):
        config = self.process.config.model_dump_json()
        logger.info(f"created {self.identifier} with configs: {config}")

    def should_partition(self, filepath: Path, file_data: FileData) -> bool:
        if self.context.reprocess or file_data.reprocess:
            return True
        return not filepath.exists()

    def get_output_filepath(self, filename: Path) -> Path:
        hashed_output_file = f"{self.get_hash(extras=[filename.name])}.json"
        filepath = (self.cache_dir / hashed_output_file).resolve()
        filepath.parent.mkdir(parents=True, exist_ok=True)
        return filepath

    def _save_output(self, output_filepath: str, partitioned_content: list[dict]):
        logger.debug(f"writing partitioned output to: {output_filepath}")
        write_data(path=Path(output_filepath), data=partitioned_content)

    async def _run_async(
        self, fn: Callable, path: str, file_data_path: str
    ) -> Optional[PartitionStepResponse]:
        path = Path(path)
        file_data = file_data_from_file(path=file_data_path)
        output_filepath = self.get_output_filepath(filename=Path(file_data_path))
        if not self.should_partition(filepath=output_filepath, file_data=file_data):
            logger.debug(f"skipping partitioning, output already exists: {output_filepath}")
            return PartitionStepResponse(file_data_path=file_data_path, path=str(output_filepath))
        fn_kwargs = {"filename": path, "metadata": file_data.metadata.model_dump()}
        if not asyncio.iscoroutinefunction(fn):
            partitioned_content = fn(**fn_kwargs)
        elif semaphore := self.context.semaphore:
            async with semaphore:
                partitioned_content = await fn(**fn_kwargs)
        else:
            partitioned_content = await fn(**fn_kwargs)
        self._save_output(
            output_filepath=str(output_filepath), partitioned_content=partitioned_content
        )
        return PartitionStepResponse(file_data_path=file_data_path, path=str(output_filepath))

    def get_hash(self, extras: Optional[list[str]]) -> str:
        hashable_string = serialize_base_model_json(
            model=self.process.config, sort_keys=True, ensure_ascii=True
        )
        if extras:
            hashable_string += "".join(extras)
        return hashlib.sha256(hashable_string.encode()).hexdigest()[:12]
