from email import message_from_string, policy
from unittest.mock import patch

import pytest

from test.integration.connectors.utils.constants import SOURCE_TAG
from unstructured_ingest.error import ValueError as IngestValueError
from unstructured_ingest.processes.connectors.salesforce import (
    CONNECTOR_TYPE,
    SalesforceAccessConfig,
    SalesforceDownloader,
)

# Secret material that must never leak into a raised error or its chained cause.
_SECRET = "BEGIN PRIVATE KEY SECRETKEYMATERIAL123"


@pytest.mark.tags(SOURCE_TAG, CONNECTOR_TYPE)
def test_get_private_key_value_redacts_parse_failure():
    access_config = SalesforceAccessConfig(consumer_key="ck", private_key="not-a-real-key")

    with (
        patch(
            "cryptography.hazmat.primitives.serialization.load_pem_private_key",
            side_effect=Exception(_SECRET),
        ),
        pytest.raises(IngestValueError) as exc_info,
    ):
        access_config.get_private_key_value_and_type()

    message = str(exc_info.value)
    assert "SECRETKEYMATERIAL123" not in message
    assert _SECRET not in message
    assert "failed to validate private key data" in message
    # raised `from None`, so the original exception is not chained
    assert exc_info.value.__cause__ is None


def _parse_eml(**bodies):
    record = {"MessageDate": "2024-01-02T03:04:05Z", "Subject": "s", **bodies}
    eml = SalesforceDownloader._eml_for_record(None, record)
    return message_from_string(eml, policy=policy.default)


@pytest.mark.tags(SOURCE_TAG, CONNECTOR_TYPE)
def test_eml_keeps_body_lines_that_look_like_headers():
    message = _parse_eml(TextBody="Update: the total is 5\nbye", HtmlBody="Note: <b>5</b>")

    parts = list(message.iter_parts())
    assert [part.get_content_type() for part in parts] == ["text/plain", "text/html"]
    assert parts[0].get_content().strip() == "Update: the total is 5\nbye"
    assert parts[1].get_content().strip() == "Note: <b>5</b>"
    assert not any(part.defects for part in message.walk())


@pytest.mark.tags(SOURCE_TAG, CONNECTOR_TYPE)
def test_eml_for_text_only_email_has_no_html_part():
    message = _parse_eml(TextBody="Hi", HtmlBody=None)

    assert message.get_body(preferencelist=("html", "plain")).get_content().strip() == "Hi"
    assert [part.get_content_type() for part in message.iter_parts()] == ["text/plain"]


@pytest.mark.tags(SOURCE_TAG, CONNECTOR_TYPE)
def test_eml_for_html_only_email_has_empty_text_part():
    message = _parse_eml(TextBody=None, HtmlBody="<p>Hi</p>")

    text, html = message.iter_parts()
    assert text.get_content().strip() == ""
    assert html.get_content().strip() == "<p>Hi</p>"
