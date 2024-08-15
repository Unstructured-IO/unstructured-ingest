import logging
from dataclasses import dataclass, field

import click
from pydantic import BaseModel

from unstructured_ingest.v2.cli.base.cmd import BaseCmd
from unstructured_ingest.v2.cli.utils.click import Group, conform_click_options
from unstructured_ingest.v2.cli.utils.model_conversion import options_from_base_model
from unstructured_ingest.v2.interfaces import ProcessorConfig
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes import (
    ChunkerConfig,
    EmbedderConfig,
    FiltererConfig,
    PartitionerConfig,
)
from unstructured_ingest.v2.processes.connector_registry import SourceRegistryEntry


@dataclass
class SrcCmd(BaseCmd):
    registry_entry: SourceRegistryEntry
    default_configs: list[BaseModel] = field(
        default_factory=lambda: [
            ProcessorConfig,
            PartitionerConfig,
            EmbedderConfig,
            FiltererConfig,
            ChunkerConfig,
        ]
    )

    def get_registry_options(self):
        options = []
        configs = [
            config
            for config in [
                self.registry_entry.connection_config,
                self.registry_entry.indexer_config,
                self.registry_entry.downloader_config,
            ]
            if config
        ]
        for config in configs:
            options.extend(options_from_base_model(model=config))
        options = self.consolidate_options(options=options)
        return options

    def cmd(self, ctx: click.Context, **options) -> None:
        if ctx.invoked_subcommand:
            return

        conform_click_options(options)
        logger.setLevel(logging.DEBUG if options.get("verbose", False) else logging.INFO)
        try:
            pipeline = self.get_pipline(src=self.cmd_name, source_options=options)
            pipeline.run()
        except Exception as e:
            logger.error(f"failed to run source command {self.cmd_name}: {e}", exc_info=True)
            raise click.ClickException(str(e)) from e

    def get_cmd(self) -> click.Group:
        # Dynamically create the command without the use of click decorators
        fn = self.cmd
        fn = click.pass_context(fn)
        cmd = click.group(fn, cls=Group)
        if not isinstance(cmd, click.core.Group):
            raise ValueError(f"generated src command was not of expected type Group: {type(cmd)}")
        cmd.name = self.cli_cmd_name
        cmd.short_help = "v2"
        cmd.invoke_without_command = True
        self.add_options(cmd)

        # TODO remove after v1 no longer supported
        cmd.params.append(
            click.Option(
                ["--output-dir"],
                required=False,
                type=str,
                help="Local path to write partitioned output to",
            )
        )
        return cmd
