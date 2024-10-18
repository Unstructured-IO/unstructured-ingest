import pytest
from pydantic import Secret, ValidationError

from unstructured_ingest.v2.interfaces import AccessConfig, ConnectionConfig


def test_failing_connection_config():
    class MyAccessConfig(AccessConfig):
        sensitive_value: str

    class MyConnectionConfig(ConnectionConfig):
        access_config: MyAccessConfig

    with pytest.raises(ValidationError):
        MyConnectionConfig(access_config=MyAccessConfig(sensitive_value="this"))


def test_happy_path_connection_config():
    class MyAccessConfig(AccessConfig):
        sensitive_value: str

    class MyConnectionConfig(ConnectionConfig):
        access_config: Secret[MyAccessConfig]

    connection_config = MyConnectionConfig(access_config=MyAccessConfig(sensitive_value="this"))
    assert connection_config
