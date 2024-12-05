import pytest
from pydantic import ValidationError

from unstructured_ingest.v2.processes.connectors.confluence import (
    ConfluenceAccessConfig,
    ConfluenceConnectionConfig,
)


def test_connection_config_multiple_auth():
    with pytest.raises(ValidationError):
        ConfluenceConnectionConfig(
            access_config=ConfluenceAccessConfig(
                api_token="api_token",
                access_token="access_token",
            ),
            user_email="user_email",
            url="url",
        )


def test_connection_config_no_auth():
    with pytest.raises(ValidationError):
        ConfluenceConnectionConfig(access_config=ConfluenceAccessConfig(), url="url")


def test_connection_config_basic_auth():
    ConfluenceConnectionConfig(
        access_config=ConfluenceAccessConfig(api_token="api_token"),
        url="url",
        user_email="user_email",
    )


def test_connection_config_pat_auth():
    ConfluenceConnectionConfig(
        access_config=ConfluenceAccessConfig(access_token="access_token"),
        url="url",
    )
