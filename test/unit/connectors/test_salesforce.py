import xml.etree.ElementTree as ET
from collections import OrderedDict
from unittest.mock import patch

import pytest

from test.integration.connectors.utils.constants import SOURCE_TAG
from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import ValueError as IngestValueError
from unstructured_ingest.processes.connectors.salesforce import (
    CONNECTOR_TYPE,
    SalesforceAccessConfig,
    SalesforceConnectionConfig,
    SalesforceDownloader,
    SalesforceDownloaderConfig,
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


@pytest.mark.tags(SOURCE_TAG, CONNECTOR_TYPE)
@pytest.mark.parametrize(("local_encoding", "name"), [("cp1252", "Montréal"), ("ascii", "東京")])
def test_download_account_xml_uses_utf8(tmp_path, local_encoding, name):
    def open_with_local_default(path, mode, *, encoding=None):
        return open(path, mode, encoding=encoding or local_encoding)

    downloader = SalesforceDownloader(
        connection_config=SalesforceConnectionConfig(
            username="test@example.com",
            access_config=SalesforceAccessConfig(consumer_key="ck", private_key="test-key"),
        ),
        download_config=SalesforceDownloaderConfig(download_dir=tmp_path),
    )
    file_data = FileData(
        identifier="001",
        connector_type=CONNECTOR_TYPE,
        source_identifiers=SourceIdentifiers(filename="001.xml", fullpath="Account/001.xml"),
        additional_metadata={"record_type": "Account"},
    )

    with (
        patch.object(
            downloader, "_get_response", return_value={"records": [OrderedDict(Name=name)]}
        ),
        patch(
            "unstructured_ingest.processes.connectors.salesforce.open",
            side_effect=open_with_local_default,
            create=True,
        ),
    ):
        response = downloader.run(file_data)

    assert ET.parse(response["path"]).getroot().findtext("item") == f"Name: {name}"
