import typing as t
from dataclasses import dataclass

import click

from unstructured_ingest.cli.base.src import BaseSrcCmd
from unstructured_ingest.cli.interfaces import (
    CliConfig,
)
from unstructured_ingest.connector.biomed import SimpleBiomedConfig


@dataclass
class BiomedCliConfig(SimpleBiomedConfig, CliConfig):
    @staticmethod
    def get_cli_options() -> t.List[click.Option]:
        options = [
            click.Option(
                ["--api-id"],
                default=None,
                help="ID parameter for OA Web Service API.",
            ),
            click.Option(
                ["--api-from"],
                default=None,
                help="From parameter for OA Web Service API.",
            ),
            click.Option(
                ["--api-until"],
                default=None,
                help="Until parameter for OA Web Service API.",
            ),
            click.Option(
                ["--path"],
                default=None,
                help="PMC Open Access FTP Directory Path.",
            ),
            click.Option(
                ["--max-request-time"],
                default=45,
                help="(In seconds) Max request time to OA Web Service API.",
            ),
        ]
        return options


def get_base_src_cmd() -> BaseSrcCmd:
    cmd_cls = BaseSrcCmd(
        cmd_name="biomed",
        cli_config=BiomedCliConfig,
    )
    return cmd_cls
