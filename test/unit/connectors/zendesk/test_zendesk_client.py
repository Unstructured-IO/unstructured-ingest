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
from unstructured_ingest.processes.connectors.zendesk.zendesk import (
    ZendeskAccessConfig,
    ZendeskConnectionConfig,
    ZendeskIndexer,
    ZendeskIndexerConfig,
)

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


def _track_instances(mocker: MockerFixture, cls: type) -> list:
    """Records every instance of ``cls`` constructed after this call."""
    instances = []
    original_init = cls.__init__

    def record(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        instances.append(self)

    mocker.patch.object(cls, "__init__", record)
    return instances


def test_constructor_does_not_open_an_async_client(mocker: MockerFixture):
    async_clients = _track_instances(mocker, httpx.AsyncClient)
    client = _client(mocker)

    assert async_clients == []
    client.close()


@pytest.mark.asyncio
async def test_async_context_closes_both_http_clients(mocker: MockerFixture):
    client = _client(mocker)
    sync_client = client._client

    async with client:
        async_client = client._async_client
        assert not async_client.is_closed

    assert async_client.is_closed
    assert sync_client.is_closed


def test_close_releases_sync_client(mocker: MockerFixture):
    client = _client(mocker)
    sync_client = client._client

    client.close()

    assert sync_client.is_closed


def test_constructor_closes_sync_client_when_precheck_fails(mocker: MockerFixture):
    mocker.patch("httpx.Client.head", side_effect=RuntimeError("connection failed"))
    sync_clients = _track_instances(mocker, httpx.Client)
    async_clients = _track_instances(mocker, httpx.AsyncClient)

    with pytest.raises(UnstructuredIngestError):
        ZendeskClient(token="tok", subdomain="sub", email="user@example.com")

    assert len(sync_clients) == 1
    assert sync_clients[0].is_closed
    assert async_clients == []


def test_precheck_leaves_no_open_clients(mocker: MockerFixture):
    mocker.patch("httpx.Client.head", return_value=mocker.MagicMock())
    sync_clients = _track_instances(mocker, httpx.Client)
    async_clients = _track_instances(mocker, httpx.AsyncClient)

    indexer = ZendeskIndexer(
        connection_config=ZendeskConnectionConfig(
            subdomain="sub",
            email="user@example.com",
            access_config=ZendeskAccessConfig(api_token="tok"),
        ),
        index_config=ZendeskIndexerConfig(),
    )
    indexer.precheck()

    assert len(sync_clients) == 1
    assert sync_clients[0].is_closed
    assert async_clients == []
