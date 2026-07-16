import httpx
import pytest
from pytest_mock import MockerFixture

from unstructured_ingest.error import (
    ProviderError,
    UnstructuredIngestError,
    UserAuthError,
    UserError,
)
from unstructured_ingest.processes.connectors.zendesk.client import ZendeskClient

SECRET = "SECRETpassword=hunter2 key=AKIAEXAMPLE"


def _client(mocker: MockerFixture) -> ZendeskClient:
    # __post_init__ runs a live HEAD precheck; stub it so construction is offline.
    mocker.patch("httpx.Client.head", return_value=mocker.MagicMock())
    return ZendeskClient(token="tok", subdomain="sub", email="user@example.com")


def _http_status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://sub.zendesk.com/api/v2/groups.json")
    response = httpx.Response(status_code, request=request, text=SECRET)
    return httpx.HTTPStatusError(SECRET, request=request, response=response)


@pytest.mark.parametrize(
    ("status_code", "expected_type"),
    [
        (401, UserAuthError),
        (403, UserError),
        (500, ProviderError),
    ],
)
def test_wrap_error_http_status_redacts(status_code, expected_type, mocker: MockerFixture):
    client = _client(mocker)
    wrapped = client.wrap_error(e=_http_status_error(status_code))

    assert isinstance(wrapped, expected_type)
    assert SECRET not in str(wrapped)
    assert "hunter2" not in str(wrapped)


def test_wrap_error_500_is_provider_error_not_raw(mocker: MockerFixture):
    client = _client(mocker)
    err = _http_status_error(500)
    wrapped = client.wrap_error(e=err)

    assert isinstance(wrapped, ProviderError)
    assert wrapped is not err


def test_wrap_error_non_http_status_redacts(mocker: MockerFixture):
    client = _client(mocker)
    wrapped = client.wrap_error(e=RuntimeError(SECRET))

    assert isinstance(wrapped, UnstructuredIngestError)
    assert SECRET not in str(wrapped)
    assert "hunter2" not in str(wrapped)
