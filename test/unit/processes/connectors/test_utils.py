import base64
import json
import zlib

from unstructured_ingest.processes.connectors.utils import format_and_truncate_orig_elements


def test_format_and_truncate_orig_elements():
    original_elements = [
        {
            "text": "Hello, world!",
            "metadata": {
                "image_base64": "iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAABwUlEQVR42mNk",
                "text_as_html": "<p>Hello, world!</p>",
                "page": 1,
            },
        }
    ]
    json_bytes = json.dumps(original_elements, sort_keys=True).encode("utf-8")
    deflated_bytes = zlib.compress(json_bytes)
    b64_deflated_bytes = base64.b64encode(deflated_bytes)
    b64_deflated_bytes.decode("utf-8")

    assert format_and_truncate_orig_elements(
        {"text": "Hello, world!", "metadata": {"orig_elements": b64_deflated_bytes.decode("utf-8")}}
    ) == [{"metadata": {"page": 1}}]
