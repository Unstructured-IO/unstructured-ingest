from unittest.mock import MagicMock

import pytest

from test.integration.connectors.utils.constants import DESTINATION_TAG, VECTOR_DB_TAG
from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.processes.connectors.chroma import (
    CONNECTOR_TYPE,
    ChromaConnectionConfig,
    ChromaUploader,
    ChromaUploaderConfig,
)

# Secret material that must never leak into a raised error or its chained cause.
_SECRET = "chroma auth token=SECRET123"


@pytest.mark.tags(DESTINATION_TAG, CONNECTOR_TYPE, VECTOR_DB_TAG)
def test_upsert_batch_redacts_upsert_failure():
    uploader = ChromaUploader(
        upload_config=ChromaUploaderConfig(collection_name="c"),
        connection_config=ChromaConnectionConfig(),
    )

    collection = MagicMock()
    collection.upsert.side_effect = Exception(_SECRET)
    batch = {"ids": [], "documents": [], "embeddings": [], "metadatas": []}

    with pytest.raises(DestinationConnectionError) as exc_info:
        uploader.upsert_batch(collection, batch)

    message = str(exc_info.value)
    assert "SECRET123" not in message
    assert _SECRET not in message
    assert "chroma error" in message
    # raised `from None`, so the original exception is not chained
    assert exc_info.value.__cause__ is None
