import logging
from dataclasses import dataclass

import click

from unstructured_ingest.v2.cli.base.cmd import BaseCmd
from unstructured_ingest.v2.cli.utils.click import Dict, conform_click_options
from unstructured_ingest.v2.cli.utils.model_conversion import options_from_base_model
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry


@dataclass
class DestCmd(BaseCmd):
    registry_entry: DestinationRegistryEntry

    def get_registry_options(self):
        options = []
        configs = [
            config
            for config in [
                self.registry_entry.uploader_config,
                self.registry_entry.upload_stager_config,
                self.registry_entry.connection_config,
            ]
            if config
        ]
        for config in configs:
            options.extend(options_from_base_model(model=config))
        options = self.consolidate_options(options=options)
        return options

    def cmd(self, ctx: click.Context, **options) -> None:
        logger.setLevel(logging.DEBUG if options.get("verbose", False) else logging.INFO)
        if not ctx.parent:
            raise click.ClickException("destination command called without a parent")
        if not ctx.parent.info_name:
            raise click.ClickException("parent command missing info name")
        source_cmd = ctx.parent.info_name.replace("-", "_")
        source_options: dict = ctx.parent.params if ctx.parent else {}
        conform_click_options(options)
        try:
            pipeline = self.get_pipline(
                src=source_cmd,
                source_options=source_options,
                dest=self.cmd_name,
                destination_options=options,
            )
            pipeline.run()
        except Exception as e:
            logger.error(f"failed to run destination command {self.cmd_name}: {e}", exc_info=True)
            raise click.ClickException(str(e)) from e

    def get_cmd(self) -> click.Command:
        # Dynamically create the command without the use of click decorators
        fn = self.cmd
        fn = click.pass_context(fn)
        cmd = click.command(fn)
        if not isinstance(cmd, click.core.Command):
            raise ValueError(f"generated command was not of expected type Command: {type(cmd)}")
        cmd.name = self.cli_cmd_name
        cmd.short_help = "v2"
        cmd.invoke_without_command = True
        self.add_options(cmd)
        cmd.params.append(
            click.Option(
                ["--custom-stager"],
                required=False,
                type=str,
                default=None,
                help="Pass a pointer to a custom upload stager to use, "
                "must be in format '<module>:<attribute>'",
            )
        )
        cmd.params.append(
            click.Option(
                ["--custom-stager-config-kwargs"],
                required=False,
                type=Dict(),
                default=None,
                help="Any kwargs to instantiate the configuration "
                "associated with the customer stager",
            )
        )
        return cmd
