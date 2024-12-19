import typing as t
from dataclasses import dataclass

import click

from unstructured_ingest.cli.interfaces import (
    CliConfig,
)
from unstructured_ingest.connector.azure_ai_search import (
    AzureAISearchWriteConfig,
    SimpleAzureAISearchStorageConfig,
)


@dataclass
class AzureAISearchCliConfig(SimpleAzureAISearchStorageConfig, CliConfig):
    @staticmethod
    def get_cli_options() -> t.List[click.Option]:
        options = [
            click.Option(
                ["--key"],
                required=True,
                type=str,
                help="Key credential used for authenticating to an Azure service.",
                envvar="AZURE_SEARCH_API_KEY",
                show_envvar=True,
            ),
            click.Option(
                ["--endpoint"],
                required=True,
                type=str,
                help="The URL endpoint of an Azure search service. "
                "In the form of https://{{service_name}}.search.windows.net",
                envvar="AZURE_SEARCH_ENDPOINT",
                show_envvar=True,
            ),
        ]
        return options


@dataclass
class AzureAISearchCliWriteConfig(AzureAISearchWriteConfig, CliConfig):
    @staticmethod
    def get_cli_options() -> t.List[click.Option]:
        options = [
            click.Option(
                ["--index"],
                required=True,
                type=str,
                help="The name of the index to connect to",
            ),
        ]
        return options


def get_base_dest_cmd():
    from unstructured_ingest.cli.base.dest import BaseDestCmd

    cmd_cls = BaseDestCmd(
        cmd_name="azure-ai-search",
        cli_config=AzureAISearchCliConfig,
        additional_cli_options=[AzureAISearchCliWriteConfig],
        write_config=AzureAISearchCliWriteConfig,
    )
    return cmd_cls
