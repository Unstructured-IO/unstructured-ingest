import os
from asyncio import Semaphore
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

DEFAULT_WORK_DIR = str((Path.home() / ".cache" / "unstructured" / "ingest" / "pipeline").resolve())


class ProcessorConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    reprocess: bool = False
    verbose: bool = False
    tqdm: bool = False
    work_dir: str = Field(default_factory=lambda: DEFAULT_WORK_DIR)
    num_processes: int = 2
    max_connections: Optional[int] = None
    raise_on_error: bool = False
    disable_parallelism: bool = Field(
        default_factory=lambda: os.getenv("INGEST_DISABLE_PARALLELISM", "false").lower() == "true"
    )
    preserve_downloads: bool = False
    download_only: bool = False
    max_docs: Optional[int] = None
    re_download: bool = False
    uncompress: bool = False

    # OTEL support
    otel_endpoint: Optional[str] = Field(
        default=None, description="OTEL endpoint to publish trace data to"
    )

    # Used to keep track of state in pipeline
    status: dict = Field(default_factory=dict)
    semaphore: Optional[Semaphore] = Field(init=False, default=None, exclude=True)

    def model_post_init(self, __context: Any) -> None:
        if self.max_connections is not None:
            self.semaphore = Semaphore(self.max_connections)

    @property
    def mp_supported(self) -> bool:
        return not self.disable_parallelism and self.num_processes > 1

    @property
    def async_supported(self) -> bool:
        if self.disable_parallelism:
            return False
        if self.max_connections is not None and isinstance(self.max_connections, int):
            return self.max_connections > 1
        return True
