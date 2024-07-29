import typing as t
from dataclasses import dataclass

import click

from unstructured_ingest.cli.base.src import BaseSrcCmd
from unstructured_ingest.cli.interfaces import (
    CliConfig,
)
from unstructured_ingest.connector.fsspec.dropbox import (
    DropboxWriteConfig,
    SimpleDropboxConfig,
)

CMD_NAME = "dropbox"


@dataclass
class DropboxCliConfig(SimpleDropboxConfig, CliConfig):
    @staticmethod
    def get_cli_options() -> t.List[click.Option]:
        options = [
            click.Option(
                ["--token"],
                required=True,
                type=str,
                help="Dropbox access token.",
            ),
        ]
        return options


def get_base_src_cmd() -> BaseSrcCmd:
    cmd_cls = BaseSrcCmd(
        cmd_name=CMD_NAME,
        cli_config=DropboxCliConfig,
        is_fsspec=True,
    )
    return cmd_cls


def get_base_dest_cmd():
    from unstructured_ingest.cli.base.dest import BaseDestCmd

    cmd_cls = BaseDestCmd(
        cmd_name=CMD_NAME,
        cli_config=DropboxCliConfig,
        write_config=DropboxWriteConfig,
        is_fsspec=True,
    )
    return cmd_cls
