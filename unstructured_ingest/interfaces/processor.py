import os
from asyncio import Semaphore
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

DEFAULT_WORK_DIR = str((Path.home() / ".cache" / "unstructured" / "ingest" / "pipeline").resolve())


class ProcessorConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    reprocess: bool = Field(
        default=False,
        description="Reprocess a downloaded file even if the relevant structured "
        "output .json file in output directory already exists.",
    )
    verbose: bool = Field(default=False)
    tqdm: bool = Field(default=False, description="Display tqdm progress bar")
    work_dir: str = Field(
        default_factory=lambda: DEFAULT_WORK_DIR,
        description="Where to place working files when processing each step",
    )
    num_processes: int = Field(
        default=2, description="Number of parallel processes with which to process docs"
    )
    max_connections: Optional[int] = Field(
        default=None, description="Limit of concurrent connectionts"
    )
    raise_on_error: bool = Field(
        default=False,
        description="Is set, will raise error if any doc in the pipeline fail. "
        "Otherwise will log error and continue with other docs",
    )
    disable_parallelism: bool = Field(
        default_factory=lambda: os.getenv("INGEST_DISABLE_PARALLELISM", "false").lower() == "true",
    )
    preserve_downloads: bool = Field(
        default=False, description="Don't delete downloaded files after process completes"
    )
    download_only: bool = Field(
        default=False, description="skip the rest of the process after files are downloaded"
    )
    re_download: bool = Field(
        default=False,
        description="If set, will re-download downloaded files "
        "regardless of if they already exist locally",
    )
    uncompress: bool = Field(
        default=False,
        description="Uncompress any archived files. Currently supporting "
        "zip and tar files based on file extension.",
    )
    iter_delete: bool = Field(
        default=False,
        description="If limited on memory, this can be enabled to delete "
        "cached content as it's used and no longer needed in the pipeline.",
    )
    delete_cache: bool = Field(
        default=False,
        description="If set, will delete the cache work directory when process finishes",
    )

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
