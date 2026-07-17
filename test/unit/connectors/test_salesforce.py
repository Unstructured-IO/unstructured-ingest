from unittest.mock import patch

import pytest

from test.integration.connectors.utils.constants import SOURCE_TAG
from unstructured_ingest.error import ValueError as IngestValueError
from unstructured_ingest.processes.connectors.salesforce import (
    CONNECTOR_TYPE,
    SalesforceAccessConfig,
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
