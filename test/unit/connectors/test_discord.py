import asyncio
import contextlib
from unittest.mock import Mock

import pytest
from pydantic import Secret

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import ValueError as IngestValueError
from unstructured_ingest.processes.connectors.discord import (
    DiscordAccessConfig,
    DiscordConnectionConfig,
    DiscordDownloader,
    DiscordDownloaderConfig,
)


class _Forbidden(Exception):
    pass


class _FakeChannel:
    name = "general"

    def __init__(self, messages=None, error=None):
        self._messages = messages or []
        self._error = error

    async def history(self, limit=None):
        if self._error:
            raise self._error
        for message in self._messages:
            yield message


class _FakeClient:
    """Mimics discord.Client: ``start`` only returns once ``close`` is called, and an
    exception raised inside an event handler is logged, not propagated."""

    def __init__(self, channel):
        self._channel = channel
        self._closed = asyncio.Event()
        self._handlers = {}
        self.close_calls = 0

    def event(self, coro):
        self._handlers[coro.__name__] = coro
        return coro

    def get_channel(self, channel_id):
        return self._channel

    async def start(self, token):
        # discord.py's Client._run_event routes handler exceptions to on_error, which only logs
        with contextlib.suppress(Exception):
            await self._handlers["on_ready"]()
        await self._closed.wait()

    async def close(self):
        self.close_calls += 1
        self._closed.set()


def _file_data():
    return FileData(
        identifier="1",
        connector_type="discord",
        source_identifiers=SourceIdentifiers(filename="1.txt", fullpath="1.txt"),
        metadata=FileDataSourceMetadata(record_locator={"channel_id": "1"}),
    )


def _downloader(monkeypatch, tmp_path, client):
    connection_config = DiscordConnectionConfig(
        access_config=Secret(DiscordAccessConfig(token="token"))
    )
    monkeypatch.setattr(DiscordConnectionConfig, "get_client", lambda self: client)
    return DiscordDownloader(
        connection_config=connection_config,
        download_config=DiscordDownloaderConfig(download_dir=tmp_path),
    )


async def test_discord_download_writes_messages(monkeypatch, tmp_path):
    channel = _FakeChannel(messages=[Mock(content="hello"), Mock(content="world")])
    downloader = _downloader(monkeypatch, tmp_path, _FakeClient(channel))

    response = await asyncio.wait_for(downloader.run_async(file_data=_file_data()), timeout=5)

    assert response["path"].read_text() == "hello\nworld"


async def test_discord_download_fails_when_channel_history_is_forbidden(monkeypatch, tmp_path):
    client = _FakeClient(_FakeChannel(error=_Forbidden("403 Forbidden (error code: 50001)")))
    downloader = _downloader(monkeypatch, tmp_path, client)

    with pytest.raises(_Forbidden):
        await asyncio.wait_for(downloader.run_async(file_data=_file_data()), timeout=5)

    assert client.close_calls >= 1


async def test_discord_download_fails_when_channel_is_not_found(monkeypatch, tmp_path):
    downloader = _downloader(monkeypatch, tmp_path, _FakeClient(None))

    with pytest.raises(IngestValueError, match="channel not found"):
        await asyncio.wait_for(downloader.run_async(file_data=_file_data()), timeout=5)
