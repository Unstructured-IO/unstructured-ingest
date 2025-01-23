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
                password="api_token",
                token="access_token",
            ),
            username="user_email",
            url="url",
        )


def test_connection_config_no_auth():
    with pytest.raises(ValidationError):
        ConfluenceConnectionConfig(access_config=ConfluenceAccessConfig(), url="url")


def test_connection_config_basic_auth():
    ConfluenceConnectionConfig(
        access_config=ConfluenceAccessConfig(password="api_token"),
        url="url",
        username="user_email",
    )


def test_connection_config_pat_auth():
    ConfluenceConnectionConfig(
        access_config=ConfluenceAccessConfig(token="access_token"),
        url="url",
    )
