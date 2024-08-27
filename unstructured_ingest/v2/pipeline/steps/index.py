import hashlib
import json
from dataclasses import dataclass
from typing import Generator, Optional, TypeVar

from unstructured_ingest.v2.interfaces.indexer import Indexer
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.pipeline.interfaces import PipelineStep
from unstructured_ingest.v2.pipeline.otel import instrument
from unstructured_ingest.v2.utils import serialize_base_model_json

IndexerT = TypeVar("IndexerT", bound=Indexer)

STEP_ID = "index"


@dataclass
class IndexStep(PipelineStep):
    process: IndexerT
    identifier: str = STEP_ID

    def __str__(self):
        return f"{self.identifier} ({self.process.__class__.__name__})"

    def __post_init__(self):
        config = self.process.index_config.json() if self.process.index_config else None
        connection_config = (
            self.process.connection_config.json() if self.process.connection_config else None
        )
        logger.info(
            f"Created {self.identifier} with configs: {config}, "
            f"connection configs: {connection_config}"
        )

    @instrument(span_name=STEP_ID)
    def run(self) -> Generator[str, None, None]:
        for file_data in self.process.run():
            logger.debug(f"Generated file data: {file_data.to_dict()}")
            try:
                record_hash = self.get_hash(extras=[file_data.identifier])
                filename = f"{record_hash}.json"
                filepath = (self.cache_dir / filename).resolve()
                filepath.parent.mkdir(parents=True, exist_ok=True)
                with open(str(filepath), "w") as f:
                    json.dump(file_data.to_dict(), f, indent=2)
                yield str(filepath)
            except Exception as e:
                logger.error(f"failed to create index for file data: {file_data}", exc_info=True)
                if self.context.raise_on_error:
                    raise e
                continue

    def get_hash(self, extras: Optional[list[str]]) -> str:
        index_config_dict = json.loads(
            serialize_base_model_json(model=self.process.index_config, sort_keys=True)
        )
        connection_config_dict = json.loads(
            serialize_base_model_json(model=self.process.connection_config, sort_keys=True)
        )
        hashable_dict = {
            "index_config": index_config_dict,
            "connection_config": connection_config_dict,
        }
        hashable_string = json.dumps(hashable_dict, sort_keys=True)
        if extras:
            hashable_string += "".join(extras)
        return hashlib.sha256(hashable_string.encode()).hexdigest()[:12]
