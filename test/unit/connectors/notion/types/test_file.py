"""Unit tests for Notion file-object parsing and its hardening.

``FileObject`` (and its ``External`` / ``File`` contents) backs the image,
video, file and PDF blocks. Notion regularly extends file payloads (signing
metadata, etc.), so these types must drop unknown keys instead of raising.
"""

from unstructured_ingest.processes.connectors.notion.types.file import (
    External,
    File,
    FileObject,
)

_FUTURE_KEYS = {"some_future_field": "x", "another_addition": {"nested": True}}


def render(tag) -> str:
    return tag.render() if tag is not None else ""


def test_external_ignores_unknown_keys():
    ext = External.from_dict({"url": "https://x/y.png", **_FUTURE_KEYS})
    assert ext.url == "https://x/y.png"
    assert not hasattr(ext, "some_future_field")


def test_file_with_expiry():
    f = File.from_dict({"url": "https://x/y.png", "expiry_time": "2099-01-01"})
    assert f.url == "https://x/y.png"
    assert f.expiry_time == "2099-01-01"


def test_file_without_expiry_is_optional():
    # expiry_time is not guaranteed on every file payload.
    f = File.from_dict({"url": "https://x/y.png"})
    assert f.url == "https://x/y.png"
    assert f.expiry_time is None


def test_file_ignores_unknown_keys():
    f = File.from_dict({"url": "https://x/y.png", **_FUTURE_KEYS})
    assert f.url == "https://x/y.png"
    assert not hasattr(f, "some_future_field")


def test_file_object_external_renders_url():
    obj = FileObject.from_dict(
        {"type": "external", "external": {"url": "https://e/x.txt", **_FUTURE_KEYS}}
    )
    assert "https://e/x.txt" in render(obj.get_html())


def test_file_object_internal_renders_url():
    obj = FileObject.from_dict(
        {
            "type": "file",
            "file": {"url": "https://n/x.txt", "expiry_time": "2099-01-01", **_FUTURE_KEYS},
        }
    )
    assert "https://n/x.txt" in render(obj.get_html())
