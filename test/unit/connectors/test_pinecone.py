from unittest.mock import MagicMock, patch

import pytest
from pinecone.exceptions import PineconeApiException

from unstructured_ingest.error import UnstructuredIngestError
from unstructured_ingest.processes.connectors.pinecone import (
    PineconeAccessConfig,
    PineconeConnectionConfig,
    PineconeUploader,
    PineconeUploaderConfig,
)


def _make_pinecone_api_exception(status: int):
    return PineconeApiException(status=status, reason="Bad Request")


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 429, 500, 503])
def test_upsert_batches_async_propagates_api_error_status_code(status_code):
    mock_async_result = MagicMock()
    mock_async_result.get.side_effect = _make_pinecone_api_exception(status=status_code)

    mock_index = MagicMock()
    mock_index.__enter__ = MagicMock(return_value=mock_index)
    mock_index.__exit__ = MagicMock(return_value=False)
    mock_index.upsert.return_value = mock_async_result

    connection_config = PineconeConnectionConfig(
        index_name="test-index",
        access_config=PineconeAccessConfig(pinecone_api_key="fake-key"),
    )
    uploader = PineconeUploader(
        connection_config=connection_config,
        upload_config=PineconeUploaderConfig(),
    )

    with (
        patch.object(PineconeConnectionConfig, "get_index", return_value=mock_index),
        pytest.raises(UnstructuredIngestError) as exc_info,
    ):
        uploader.upsert_batches_async(
            elements_dict=[{"id": "1", "values": [0.1, 0.2], "metadata": {}}],
        )

    assert exc_info.value.status_code == status_code
    assert "http error:" in str(exc_info.value)
