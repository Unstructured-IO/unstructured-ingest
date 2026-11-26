import pytest

from unstructured_ingest.processes.connectors.notion.types.blocks import Callout


@pytest.mark.parametrize(
    "icon",
    [
        {
            "type": "custom_emoji",
            "custom_emoji": {
                "id": "45ce454c-d427-4f53-9489-e5d0f3d1db6b",
                "name": "bufo",
                "url": "https://example.com/bufo.png",
            },
        },
        {"type": "icon", "icon": {"name": "bufo", "color": "blue"}},
    ],
)
def test_callout_with_named_icon(icon: dict):
    block = Callout.from_dict({"color": "default", "icon": icon, "rich_text": []})

    assert ":bufo:" in block.get_html().render()
