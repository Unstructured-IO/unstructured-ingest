import inspect
from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass, field, fields
from typing import Any, Optional, Type, TypeVar

import click
from pydantic import BaseModel

from unstructured_ingest.v2.cli.base.importer import import_from_string
from unstructured_ingest.v2.cli.utils.click import extract_config
from unstructured_ingest.v2.cli.utils.model_conversion import options_from_base_model, post_check
from unstructured_ingest.v2.interfaces import ProcessorConfig
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.pipeline.pipeline import Pipeline
from unstructured_ingest.v2.processes.chunker import Chunker, ChunkerConfig
from unstructured_ingest.v2.processes.connector_registry import (
    DownloaderT,
    IndexerT,
    RegistryEntry,
    UploaderT,
    UploadStager,
    UploadStagerConfig,
    UploadStagerT,
    destination_registry,
    source_registry,
)
from unstructured_ingest.v2.processes.connectors.local import LocalUploader, LocalUploaderConfig
from unstructured_ingest.v2.processes.embedder import Embedder, EmbedderConfig
from unstructured_ingest.v2.processes.filter import Filterer, FiltererConfig
from unstructured_ingest.v2.processes.partitioner import Partitioner, PartitionerConfig

CommandT = TypeVar("CommandT", bound=click.Command)


@dataclass
class BaseCmd(ABC):
    cmd_name: str
    registry_entry: RegistryEntry
    default_configs: list[Type[BaseModel]] = field(default_factory=list)

    @abstractmethod
    def get_registry_options(self):
        pass

    def get_default_options(self) -> list[click.Option]:
        options = []
        for extra in self.default_configs:
            options.extend(options_from_base_model(model=extra))
        return options

    @classmethod
    def consolidate_options(cls, options: list[click.Option]) -> list[click.Option]:
        option_names = [option.name for option in options]
        duplicate_names = [name for name, count in Counter(option_names).items() if count > 1]
        if not duplicate_names:
            return options
        consolidated_options = []
        current_names = []
        for option in options:
            if option.name not in current_names:
                current_names.append(option.name)
                consolidated_options.append(option)
                continue
            existing_option = next(o for o in consolidated_options if o.name == option.name)
            if existing_option.__dict__ == option.__dict__:
                continue
            option_diff = cls.get_options_diff(o1=option, o2=existing_option)
            raise ValueError(
                "Conflicting duplicate {} option defined: {}".format(
                    option.name, " | ".join([f"{d[0]}: {d[1]}" for d in option_diff])
                )
            )
        return consolidated_options

    @staticmethod
    def get_options_diff(o1: click.Option, o2: click.Option):
        o1_dict = o1.__dict__
        o2_dict = o2.__dict__
        for d in [o1_dict, o2_dict]:
            d["opts"] = ",".join(d["opts"])
            d["secondary_opts"] = ",".join(d["secondary_opts"])
        option_diff = set(o1_dict.items()) ^ set(o2_dict.items())
        return option_diff

    @property
    def cmd_name_key(self):
        return self.cmd_name.replace("-", "_")

    @property
    def cli_cmd_name(self):
        return self.cmd_name.replace("_", "-")

    @abstractmethod
    def cmd(self, ctx: click.Context, **options) -> None:
        pass

    def add_options(self, cmd: CommandT) -> CommandT:
        options = self.get_registry_options()
        options.extend(self.get_default_options())
        post_check(options)
        cmd.params.extend(options)
        return cmd

    def get_pipline(
        self,
        src: str,
        source_options: dict[str, Any],
        dest: Optional[str] = None,
        destination_options: Optional[dict[str, Any]] = None,
    ) -> Pipeline:
        logger.debug(
            f"creating pipeline from cli using source {src} with options: {source_options}"
        )
        pipeline_kwargs: dict[str, Any] = {
            "context": self.get_processor_config(options=source_options),
            "downloader": self.get_downloader(src=src, options=source_options),
            "indexer": self.get_indexer(src=src, options=source_options),
            "partitioner": self.get_partitioner(options=source_options),
        }
        if chunker := self.get_chunker(options=source_options):
            pipeline_kwargs["chunker"] = chunker
        if filterer := self.get_filterer(options=source_options):
            pipeline_kwargs["filterer"] = filterer
        if embedder := self.get_embeder(options=source_options):
            pipeline_kwargs["embedder"] = embedder
        if dest:
            logger.debug(
                f"setting destination on pipeline {dest} with options: {destination_options}"
            )
            if uploader_stager := self.get_upload_stager(dest=dest, options=destination_options):
                pipeline_kwargs["stager"] = uploader_stager
            pipeline_kwargs["uploader"] = self.get_uploader(dest=dest, options=destination_options)
        else:
            # Default to local uploader
            # TODO remove after v1 no longer supported
            destination_options = destination_options or {}
            if "output_dir" not in destination_options:
                destination_options["output_dir"] = source_options["output_dir"]
            pipeline_kwargs["uploader"] = self.get_default_uploader(options=destination_options)
        return Pipeline(**pipeline_kwargs)

    @staticmethod
    def get_default_uploader(options: dict[str, Any]) -> UploaderT:
        uploader_config = extract_config(flat_data=options, config=LocalUploaderConfig)
        return LocalUploader(upload_config=uploader_config)

    @staticmethod
    def get_chunker(options: dict[str, Any]) -> Optional[Chunker]:
        chunker_config = extract_config(flat_data=options, config=ChunkerConfig)
        if not chunker_config.chunking_strategy:
            return None
        return Chunker(config=chunker_config)

    @staticmethod
    def get_filterer(options: dict[str, Any]) -> Optional[Filterer]:
        filterer_configs = extract_config(flat_data=options, config=FiltererConfig)
        if not filterer_configs.dict():
            return None
        return Filterer(config=filterer_configs)

    @staticmethod
    def get_embeder(options: dict[str, Any]) -> Optional[Embedder]:
        embedder_config = extract_config(flat_data=options, config=EmbedderConfig)
        if not embedder_config.embedding_provider:
            return None
        return Embedder(config=embedder_config)

    @staticmethod
    def get_partitioner(options: dict[str, Any]) -> Partitioner:
        partitioner_config = extract_config(flat_data=options, config=PartitionerConfig)
        return Partitioner(config=partitioner_config)

    @staticmethod
    def get_processor_config(options: dict[str, Any]) -> ProcessorConfig:
        return extract_config(flat_data=options, config=ProcessorConfig)

    @staticmethod
    def get_indexer(src: str, options: dict[str, Any]) -> IndexerT:
        source_entry = source_registry[src]
        indexer_kwargs: dict[str, Any] = {}
        if indexer_config_cls := source_entry.indexer_config:
            indexer_kwargs["index_config"] = extract_config(
                flat_data=options, config=indexer_config_cls
            )
        if connection_config_cls := source_entry.connection_config:
            indexer_kwargs["connection_config"] = extract_config(
                flat_data=options, config=connection_config_cls
            )
        indexer_cls = source_entry.indexer
        return indexer_cls(**indexer_kwargs)

    @staticmethod
    def get_downloader(src: str, options: dict[str, Any]) -> DownloaderT:
        source_entry = source_registry[src]
        downloader_kwargs: dict[str, Any] = {}
        if downloader_config_cls := source_entry.downloader_config:
            downloader_kwargs["download_config"] = extract_config(
                flat_data=options, config=downloader_config_cls
            )
        if connection_config_cls := source_entry.connection_config:
            downloader_kwargs["connection_config"] = extract_config(
                flat_data=options, config=connection_config_cls
            )
        downloader_cls = source_entry.downloader
        return downloader_cls(**downloader_kwargs)

    @staticmethod
    def get_custom_stager(
        stager_reference: str, stager_config_kwargs: Optional[dict] = None
    ) -> Optional[UploadStagerT]:
        uploader_cls = import_from_string(stager_reference)
        if not inspect.isclass(uploader_cls):
            raise ValueError(
                f"custom stager must be a reference to a python class, got: {type(uploader_cls)}"
            )
        if not issubclass(uploader_cls, UploadStager):
            raise ValueError(
                "custom stager must be an implementation of the UploadStager interface"
            )
        fields_dict = {f.name: f.type for f in fields(uploader_cls)}
        upload_stager_config_cls = fields_dict["upload_stager_config"]
        if not inspect.isclass(upload_stager_config_cls):
            raise ValueError(
                f"custom stager config must be a class, got: {type(upload_stager_config_cls)}"
            )
        if not issubclass(upload_stager_config_cls, UploadStagerConfig):
            raise ValueError(
                "custom stager config must be an implementation "
                "of the UploadStagerUploadStagerConfig interface"
            )
        upload_stager_kwargs: dict[str, Any] = {}
        if stager_config_kwargs:
            upload_stager_kwargs["upload_stager_config"] = upload_stager_config_cls(
                **stager_config_kwargs
            )
        return uploader_cls(**upload_stager_kwargs)

    @staticmethod
    def get_upload_stager(dest: str, options: dict[str, Any]) -> Optional[UploadStagerT]:
        if custom_stager := options.get("custom_stager"):
            return BaseCmd.get_custom_stager(
                stager_reference=custom_stager,
                stager_config_kwargs=options.get("custom_stager_config_kwargs"),
            )
        dest_entry = destination_registry[dest]
        upload_stager_kwargs: dict[str, Any] = {}
        if upload_stager_config_cls := dest_entry.upload_stager_config:
            upload_stager_kwargs["upload_stager_config"] = extract_config(
                flat_data=options, config=upload_stager_config_cls
            )
        if upload_stager_cls := dest_entry.upload_stager:
            return upload_stager_cls(**upload_stager_kwargs)
        return None

    @staticmethod
    def get_uploader(dest, options: dict[str, Any]) -> UploaderT:
        dest_entry = destination_registry[dest]
        uploader_kwargs: dict[str, Any] = {}
        if uploader_config_cls := dest_entry.uploader_config:
            uploader_kwargs["upload_config"] = extract_config(
                flat_data=options, config=uploader_config_cls
            )
        if connection_config_cls := dest_entry.connection_config:
            uploader_kwargs["connection_config"] = extract_config(
                flat_data=options, config=connection_config_cls
            )
        uploader_cls = dest_entry.uploader
        return uploader_cls(**uploader_kwargs)
