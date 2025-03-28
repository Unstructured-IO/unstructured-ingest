from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
import shutil
from dataclasses import InitVar, dataclass, field
from pathlib import Path
from typing import Any

from unstructured_ingest.interfaces import ProcessorConfig, Uploader
from unstructured_ingest.logger import logger, make_default_logger
from unstructured_ingest.otel import OtelHandler
from unstructured_ingest.pipeline.interfaces import PipelineStep
from unstructured_ingest.pipeline.steps.chunk import Chunker, ChunkStep
from unstructured_ingest.pipeline.steps.download import DownloaderT, DownloadStep
from unstructured_ingest.pipeline.steps.embed import Embedder, EmbedStep
from unstructured_ingest.pipeline.steps.filter import Filterer, FilterStep
from unstructured_ingest.pipeline.steps.index import IndexerT, IndexStep
from unstructured_ingest.pipeline.steps.partition import Partitioner, PartitionStep
from unstructured_ingest.pipeline.steps.stage import UploadStager, UploadStageStep
from unstructured_ingest.pipeline.steps.uncompress import Uncompressor, UncompressStep
from unstructured_ingest.pipeline.steps.upload import UploadStep
from unstructured_ingest.processes.chunker import ChunkerConfig
from unstructured_ingest.processes.connector_registry import (
    ConnectionConfig,
    DownloaderConfigT,
    IndexerConfigT,
    UploaderConfigT,
    UploadStagerConfigT,
    destination_registry,
    source_registry,
)
from unstructured_ingest.processes.connectors.local import LocalUploader
from unstructured_ingest.processes.embedder import EmbedderConfig
from unstructured_ingest.processes.filter import FiltererConfig
from unstructured_ingest.processes.partitioner import PartitionerConfig


class PipelineError(Exception):
    pass


@dataclass
class Pipeline:
    context: ProcessorConfig

    indexer: InitVar[IndexerT]
    indexer_step: IndexStep = field(init=False)

    downloader: InitVar[DownloaderT]
    downloader_step: DownloadStep = field(init=False)

    partitioner: InitVar[Partitioner]
    partitioner_step: PartitionStep = field(init=False)

    chunker: InitVar[Chunker | None] = None
    chunker_step: ChunkStep | None = field(init=False, default=None)

    embedder: InitVar[Embedder | None] = None
    embedder_step: EmbedStep | None = field(init=False, default=None)

    stager: InitVar[UploadStager | None] = None
    stager_step: UploadStageStep | None = field(init=False, default=None)

    uploader: InitVar[Uploader] = field(default=LocalUploader())
    uploader_step: UploadStep | None = field(init=False, default=None)

    uncompress_step: UncompressStep | None = field(init=False, default=None)

    filterer: InitVar[Filterer | None] = None
    filter_step: FilterStep | None = field(init=False, default=None)

    def __post_init__(
        self,
        indexer: IndexerT,
        downloader: DownloaderT,
        partitioner: Partitioner,
        chunker: Chunker | None = None,
        embedder: Embedder | None = None,
        stager: UploadStager | None = None,
        uploader: Uploader | None = None,
        filterer: Filterer | None = None,
    ):
        make_default_logger(level=logging.DEBUG if self.context.verbose else logging.INFO)
        otel_handler = OtelHandler(otel_endpoint=self.context.otel_endpoint)
        otel_handler.init_trace()
        self.indexer_step = IndexStep(process=indexer, context=self.context)
        self.downloader_step = DownloadStep(process=downloader, context=self.context)
        self.filter_step = FilterStep(process=filterer, context=self.context) if filterer else None
        self.partitioner_step = PartitionStep(process=partitioner, context=self.context)
        self.chunker_step = ChunkStep(process=chunker, context=self.context) if chunker else None

        self.embedder_step = EmbedStep(process=embedder, context=self.context) if embedder else None

        self.stager_step = UploadStageStep(process=stager, context=self.context) if stager else None
        self.uploader_step = UploadStep(process=uploader, context=self.context)
        if self.context.uncompress:
            process = Uncompressor()
            self.uncompress_step = UncompressStep(process=process, context=self.context)

        self.check_destination_connector()

    def check_destination_connector(self):
        # Make sure that if the set destination connector expects a stager, one is also set
        if not self.uploader_step:
            return
        uploader_connector_type = self.uploader_step.process.connector_type
        registry_entry = destination_registry[uploader_connector_type]
        if registry_entry.upload_stager and self.stager_step is None:
            try:
                self.stager_step = UploadStageStep(
                    process=registry_entry.upload_stager(), context=self.context
                )
                return
            except Exception as e:
                logger.debug(f"failed to instantiate required stager on user's behalf: {e}")
            raise ValueError(
                f"pipeline with uploader type {self.uploader_step.process.__class__.__name__} "
                f"expects a stager of type {registry_entry.upload_stager.__name__} "
                f"but one was not set"
            )

    def cleanup(self):
        if self.context.delete_cache and Path(self.context.work_dir).exists():
            logger.info(f"deleting cache directory: {self.context.work_dir}")
            shutil.rmtree(self.context.work_dir)

    def log_statuses(self):
        if status := self.context.status:
            logger.error(f"{len(status)} failed documents:")
            for k, v in status.items():
                for kk, vv in v.items():
                    logger.error(f"{k}: [{kk}] {vv}")

    def _run_initialization(self):
        failures = {}
        init_kwargs = {}
        for step in self._get_ordered_steps():
            try:
                step.process.init(**init_kwargs)
                step.process.precheck()
                # Make sure embedder dimensions available for downstream steps
                if isinstance(step.process, Embedder):
                    embed_dimensions = step.process.config.get_embedder().dimension
                    init_kwargs["vector_length"] = embed_dimensions

            except Exception as e:
                failures[step.process.__class__.__name__] = f"[{type(e).__name__}] {e}"
        if failures:
            for k, v in failures.items():
                logger.error(f"Step initialization failure: {k}: {v}")
            raise PipelineError("Initialization failed")

    def run(self):
        otel_handler = OtelHandler(otel_endpoint=self.context.otel_endpoint, log_out=logger.info)
        try:
            with otel_handler.get_tracer().start_as_current_span(
                "ingest process", record_exception=True
            ):
                self._run_initialization()
                self._run()
        finally:
            self.log_statuses()
            self.cleanup()
            if self.context.status:
                raise PipelineError("Pipeline did not run successfully")

    def clean_results(self, results: list[Any | list[Any]] | None) -> list[Any] | None:
        if not results:
            return None
        results = [r for r in results if r]
        flat = []
        for r in results:
            if isinstance(r, list):
                flat.extend(r)
            else:
                flat.append(r)
        final = [f for f in flat if f]
        return final or None

    def _get_ordered_steps(self) -> list[PipelineStep]:
        steps = [self.indexer_step, self.downloader_step]
        if self.uncompress_step:
            steps.append(self.uncompress_step)
        steps.append(self.partitioner_step)
        if self.chunker_step:
            steps.append(self.chunker_step)
        if self.embedder_step:
            steps.append(self.embedder_step)
        if self.stager_step:
            steps.append(self.stager_step)
        steps.append(self.uploader_step)
        return steps

    def apply_filter(self, records: list[dict]) -> list[dict]:
        if not self.filter_step:
            return records
        data_to_filter = [{"file_data_path": i["file_data_path"]} for i in records]
        filtered_data = self.filter_step(data_to_filter)
        filtered_data = [f for f in filtered_data if f is not None]
        filtered_file_data_paths = [r["file_data_path"] for r in filtered_data]
        filtered_records = [r for r in records if r["file_data_path"] in filtered_file_data_paths]
        return filtered_records

    def get_indices(self) -> list[dict]:
        if self.indexer_step.process.is_async():

            async def run_async():
                output = []
                async for i in self.indexer_step.run_async():
                    output.append(i)
                return output

            indices = asyncio.run(run_async())
        else:
            indices = self.indexer_step.run()
        indices_inputs = [{"file_data_path": i} for i in indices]
        return indices_inputs

    def _run(self):
        logger.info(
            f"running local pipeline: {self} with configs: {self.context.model_dump_json()}"
        )
        if self.context.mp_supported:
            manager = mp.Manager()
            self.context.status = manager.dict()
        else:
            self.context.status = {}

        # Index into data source
        indices_inputs = self.get_indices()
        if not indices_inputs:
            logger.info("No files to process after indexer, exiting")
            return

        # Initial filtering on indexed content
        indices_inputs = self.apply_filter(records=indices_inputs)
        if not indices_inputs:
            logger.info("No files to process after filtering indexed content, exiting")
            return

        # Download associated content to local file system
        downloaded_data = self.downloader_step(indices_inputs)
        downloaded_data = self.clean_results(results=downloaded_data)
        if not downloaded_data:
            logger.info("No files to process after downloader, exiting")
            return

        # Post download filtering
        downloaded_data = self.apply_filter(records=downloaded_data)
        if not downloaded_data:
            logger.info("No files to process after filtering downloaded content, exiting")
            return

        # Run uncompress if available
        if self.uncompress_step:
            downloaded_data = self.uncompress_step(downloaded_data)
            # Flatten list of lists
            downloaded_data = self.clean_results(results=downloaded_data)

            # Post uncompress filtering
            downloaded_data = self.apply_filter(records=downloaded_data)
            if not downloaded_data:
                logger.info("No files to process after filtering uncompressed content, exiting")
                return

        if not downloaded_data or self.context.download_only:
            return

        # Partition content
        elements = self.partitioner_step(downloaded_data)
        elements = self.clean_results(results=elements)
        # Download data non longer needed, delete if possible
        self.downloader_step.delete_cache()
        elements = self.clean_results(results=elements)
        if not elements:
            logger.info("No files to process after partitioning, exiting")
            return

        # Run element specific modifiers
        last_step = self.partitioner_step
        for step in [s for s in [self.chunker_step, self.embedder_step, self.stager_step] if s]:
            elements = step(elements)
            elements = self.clean_results(results=elements)
            # Delete data from previous step if possible since no longer needed
            last_step.delete_cache()
            last_step = step
            if not elements:
                logger.info(f"no files to process after {step.__class__.__name__}, exiting")
                return

        # Upload the final result
        self.uploader_step(iterable=elements)
        last_step.delete_cache()

    def __str__(self):
        s = [str(self.indexer_step)]
        if filter_step := self.filter_step:
            s.append(str(filter_step))
        s.append(str(self.downloader_step))
        if filter_step := self.filter_step:
            s.append(str(filter_step))
        if uncompress_step := self.uncompress_step:
            s.extend([str(uncompress_step), str(filter_step)])
        s.append(str(self.partitioner_step))
        if chunker_step := self.chunker_step:
            s.append(str(chunker_step))
        if embedder_step := self.embedder_step:
            s.append(str(embedder_step))
        if stager_step := self.stager_step:
            s.append(str(stager_step))
        s.append(str(self.uploader_step))
        return " -> ".join(s)

    @classmethod
    def from_configs(
        cls,
        context: ProcessorConfig,
        indexer_config: IndexerConfigT,
        downloader_config: DownloaderConfigT,
        source_connection_config: ConnectionConfig,
        partitioner_config: PartitionerConfig,
        filterer_config: FiltererConfig | None = None,
        chunker_config: ChunkerConfig | None = None,
        embedder_config: EmbedderConfig | None = None,
        destination_connection_config: ConnectionConfig | None = None,
        stager_config: UploadStagerConfigT | None = None,
        uploader_config: UploaderConfigT | None = None,
    ) -> "Pipeline":
        # Get registry key based on indexer config
        source_entry = {
            k: v
            for k, v in source_registry.items()
            if type(indexer_config) is v.indexer_config
            and type(downloader_config) is v.downloader_config
            and type(source_connection_config) is v.connection_config
        }
        if len(source_entry) > 1:
            raise ValueError(
                f"multiple entries found matching provided indexer, "
                f"downloader and connection configs: {source_entry}"
            )
        if len(source_entry) != 1:
            raise ValueError(
                "no entry found in source registry with matching indexer, "
                "downloader and connection configs"
            )
        source = list(source_entry.values())[0]
        pipeline_kwargs = {
            "context": context,
            "indexer": source.indexer(
                index_config=indexer_config, connection_config=source_connection_config
            ),
            "downloader": source.downloader(
                download_config=downloader_config, connection_config=source_connection_config
            ),
            "partitioner": Partitioner(config=partitioner_config),
        }
        if filterer_config:
            pipeline_kwargs["filterer"] = Filterer(config=filterer_config)
        if chunker_config:
            pipeline_kwargs["chunker"] = Chunker(config=chunker_config)
        if embedder_config:
            pipeline_kwargs["embedder"] = Embedder(config=embedder_config)
        if not uploader_config:
            return Pipeline(**pipeline_kwargs)

        destination_entry = {
            k: v
            for k, v in destination_registry.items()
            if isinstance(uploader_config, v.uploader_config)
        }
        if destination_connection_config:
            destination_entry = {
                k: v
                for k, v in destination_entry.items()
                if isinstance(destination_connection_config, v.connection_config)
            }
        if stager_config:
            destination_entry = {
                k: v
                for k, v in destination_entry.items()
                if isinstance(stager_config, v.upload_stager_config)
            }

        if len(destination_entry) > 1:
            raise ValueError(
                f"multiple entries found matching provided uploader, "
                f"stager and connection configs: {destination_entry}"
            )
        if len(destination_entry) != 1:
            raise ValueError(
                "no entry found in destination registry with matching uploader, "
                "stager and connection configs"
            )

        destination = list(destination_entry.values())[0]
        if stager_config:
            pipeline_kwargs["stager"] = destination.upload_stager(
                upload_stager_config=stager_config
            )
        if uploader_config:
            uploader_kwargs = {"upload_config": uploader_config}
            if destination_connection_config:
                uploader_kwargs["connection_config"] = destination_connection_config
            pipeline_kwargs["uploader"] = destination.uploader(**uploader_kwargs)
        return cls(**pipeline_kwargs)
