"""Downloaded content must be written as UTF-8, whatever the platform default encoding is."""

import asyncio
import builtins
import io
from unittest.mock import MagicMock

import pytest

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)

TEXT = "café – 日本語 🚀"


@pytest.fixture(autouse=True)
def cp1252_default(monkeypatch):
    """Make text-mode opens without an explicit encoding behave as on Windows."""
    real_open = io.open

    def fake_open(file, mode="r", buffering=-1, encoding=None, *args, **kwargs):
        if "b" not in mode and encoding in (None, "locale"):
            encoding = "cp1252"
        return real_open(file, mode, buffering, encoding, *args, **kwargs)

    monkeypatch.setattr(io, "open", fake_open)
    monkeypatch.setattr(builtins, "open", fake_open)


def _file_data(**kwargs) -> FileData:
    return FileData(
        identifier="1",
        connector_type="test",
        source_identifiers=SourceIdentifiers(filename="out.txt", fullpath="out.txt"),
        metadata=FileDataSourceMetadata(),
        **kwargs,
    )


def test_jira_download_is_utf8(tmp_path, monkeypatch):
    from unstructured_ingest.processes.connectors.jira import JiraDownloader

    downloader = MagicMock()
    downloader.form_templated_string.return_value = TEXT
    downloader.get_download_path.return_value = tmp_path / "out.txt"
    downloader.download_config.download_attachments = False
    monkeypatch.setattr(
        "unstructured_ingest.processes.connectors.jira.nested_object_to_field_getter",
        lambda fields: fields,
    )

    JiraDownloader.run(downloader, _file_data(additional_metadata={"key": "A-1"}))

    assert (tmp_path / "out.txt").read_bytes().decode("utf-8") == TEXT


def test_kafka_download_is_utf8(tmp_path):
    from unstructured_ingest.processes.connectors.kafka.kafka import KafkaDownloader

    downloader = MagicMock()
    downloader.download_dir = tmp_path

    KafkaDownloader.run(downloader, _file_data(additional_metadata={"content": TEXT}))

    assert (tmp_path / "out.txt").read_bytes().decode("utf-8") == TEXT


def test_discord_download_is_utf8(tmp_path):
    from unstructured_ingest.processes.connectors.discord import DiscordDownloader

    class Client:
        def __init__(self):
            self.handlers = {}

        def event(self, coro):
            self.handlers[coro.__name__] = coro
            return coro

        def get_channel(self, channel_id):
            async def history(limit=None):
                yield MagicMock(content=TEXT)

            return MagicMock(history=history)

        async def start(self, token):
            await self.handlers["on_ready"]()

        async def close(self):
            pass

    downloader = MagicMock()
    downloader.connection_config.get_client.return_value = Client()
    downloader.get_download_path.return_value = tmp_path / "out.txt"
    file_data = _file_data()
    file_data.metadata.record_locator = {"channel_id": "2"}

    asyncio.run(DiscordDownloader.run_async(downloader, file_data))

    assert (tmp_path / "out.txt").read_bytes().decode("utf-8") == TEXT


@pytest.mark.parametrize("kind", ["page", "database"])
def test_notion_download_is_utf8(tmp_path, monkeypatch, kind):
    pytest.importorskip("htmlBuilder")
    from unstructured_ingest.processes.connectors.notion import helpers
    from unstructured_ingest.processes.connectors.notion.connector import NotionDownloader

    extraction = MagicMock()
    extraction.html.render.return_value = TEXT
    monkeypatch.setattr(helpers, f"extract_{kind}_html", lambda **kwargs: extraction)
    downloader = MagicMock()
    downloader.get_download_path.return_value = tmp_path / "out.html"
    download = getattr(NotionDownloader, f"download_{kind}")

    download(downloader, MagicMock(), "id", _file_data())

    assert (tmp_path / "out.html").read_bytes().decode("utf-8") == TEXT
