import click

from unstructured_ingest.cli.base import DestCmd, SrcCmd
from unstructured_ingest.processes.connector_registry import (
    destination_registry,
    source_registry,
)

src_cmds = [SrcCmd(cmd_name=k, registry_entry=v) for k, v in source_registry.items()]
dest_cmds = [DestCmd(cmd_name=k, registry_entry=v) for k, v in destination_registry.items()]

src: list[click.Group] = [v.get_cmd() for v in src_cmds]

dest: list[click.Command] = [v.get_cmd() for v in dest_cmds]
