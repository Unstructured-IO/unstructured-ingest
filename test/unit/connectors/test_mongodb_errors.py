from unittest.mock import MagicMock

import pytest

pytest.importorskip("pymongo")

from pymongo.errors import (  # noqa: E402 - import after importorskip is intentional
    AutoReconnect,
    BulkWriteError,
    NetworkTimeout,
    OperationFailure,
    ServerSelectionTimeoutError,
)

from unstructured_ingest.error import (
    DestinationConnectionError,
    QuotaError,
    TimeoutError,
    WriteError,
)
from unstructured_ingest.processes.connectors.mongodb import (
    MongoDBConnectionConfig,
    MongoDBUploader,
    MongoDBUploaderConfig,
)


def _make_uploader():
    connection_config = MagicMock(spec=MongoDBConnectionConfig)
    connection_config.host = "test_host"
    upload_config = MagicMock(spec=MongoDBUploaderConfig)
    upload_config.record_id_key = "record_id"
    upload_config.database = "test_db"
    upload_config.collection = "test_collection"
    upload_config.batch_size = 100
    return MongoDBUploader(
        connection_config=connection_config,
        upload_config=upload_config,
    )


def _mock_client(uploader, collection_side_effect=None):
    mock_client = MagicMock()
    uploader.connection_config.get_client.return_value.__enter__ = MagicMock(
        return_value=mock_client
    )
    uploader.connection_config.get_client.return_value.__exit__ = MagicMock(return_value=False)
    mock_collection = mock_client.__getitem__("test_db").__getitem__("test_collection")
    # Make can_delete return False so we skip delete_by_record_id and go straight to insert
    mock_collection.list_indexes.return_value = []
    if collection_side_effect:
        mock_collection.insert_many.side_effect = collection_side_effect
    return mock_collection


class TestRunDataErrorHandling:
    def test_operation_failure_quota_raises_quota_error(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        _mock_client(uploader, OperationFailure("quota exceeded for writes"))

        with pytest.raises(QuotaError):
            uploader.run_data(data=[{"key": "value"}], file_data=file_data)

    def test_operation_failure_other_raises_destination_error(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        _mock_client(uploader, OperationFailure("some other failure"))

        with pytest.raises(DestinationConnectionError):
            uploader.run_data(data=[{"key": "value"}], file_data=file_data)

    def test_server_selection_timeout_raises_timeout_error(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        _mock_client(uploader, ServerSelectionTimeoutError("timeout"))

        with pytest.raises(TimeoutError):
            uploader.run_data(data=[{"key": "value"}], file_data=file_data)

    def test_bulk_write_error_raises_write_error(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        _mock_client(uploader, BulkWriteError({"writeErrors": [{"errmsg": "fail"}]}))

        with pytest.raises(WriteError):
            uploader.run_data(data=[{"key": "value"}], file_data=file_data)

    def test_auto_reconnect_raises_destination_error(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        _mock_client(uploader, AutoReconnect("connection lost"))

        with pytest.raises(DestinationConnectionError):
            uploader.run_data(data=[{"key": "value"}], file_data=file_data)

    def test_network_timeout_raises_timeout_error(self):
        # NetworkTimeout is a subclass of AutoReconnect; must be caught first.
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        _mock_client(uploader, NetworkTimeout("socket timeout during insert"))

        with pytest.raises(TimeoutError):
            uploader.run_data(data=[{"key": "value"}], file_data=file_data)

    def test_operation_failure_uses_structured_errmsg_for_quota(self):
        # Quota detection should match the server's errmsg field, not str(e).
        # An auth failure mentioning a database named 'quota_db' must NOT
        # be misclassified as QuotaError.
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        false_positive = OperationFailure(
            "auth failed",
            code=18,
            details={
                "errmsg": "authentication failed",
                "code": 18,
                "codeName": "AuthenticationFailed",
                "ns": "quota_db.records",
            },
        )
        _mock_client(uploader, false_positive)

        with pytest.raises(DestinationConnectionError):
            uploader.run_data(data=[{"key": "value"}], file_data=file_data)

    def test_operation_failure_structured_quota_errmsg_raises_quota_error(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        structured_quota = OperationFailure(
            "user is over quota",
            code=8000,
            details={"errmsg": "user is over quota", "code": 8000, "codeName": "AtlasError"},
        )
        _mock_client(uploader, structured_quota)

        with pytest.raises(QuotaError):
            uploader.run_data(data=[{"key": "value"}], file_data=file_data)

    def test_non_pymongo_exception_propagates_unchanged(self):
        # KeyError/TypeError/ValueError from config or input-shape problems
        # should not be relabeled as DestinationConnectionError. The narrowed
        # `except PyMongoError` lets them propagate so they're classified by
        # whatever caller-side logic exists upstream.
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        _mock_client(uploader, KeyError("missing config key"))

        with pytest.raises(KeyError):
            uploader.run_data(data=[{"key": "value"}], file_data=file_data)

    def test_delete_quota_error_not_relabeled_by_run_data_catch_all(self):
        # When can_delete() is True, run_data calls delete_by_record_id inside
        # its try block. If delete raises QuotaError, run_data's catch-all
        # must NOT relabel it as DestinationConnectionError.
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        mock_client = MagicMock()
        uploader.connection_config.get_client.return_value.__enter__ = MagicMock(
            return_value=mock_client
        )
        uploader.connection_config.get_client.return_value.__exit__ = MagicMock(return_value=False)
        mock_collection = mock_client.__getitem__("test_db").__getitem__("test_collection")
        # can_delete() returns True
        mock_collection.list_indexes.return_value = [{"key": {"record_id": 1}}]
        # delete_many raises a quota OperationFailure
        mock_collection.delete_many.side_effect = OperationFailure(
            "over quota",
            code=8000,
            details={"errmsg": "user is over quota", "code": 8000, "codeName": "AtlasError"},
        )

        with pytest.raises(QuotaError):
            uploader.run_data(data=[{"key": "value"}], file_data=file_data)


class TestDeleteByRecordIdErrorHandling:
    def test_operation_failure_quota_raises_quota_error(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        collection = MagicMock()
        collection.delete_many.side_effect = OperationFailure("quota exceeded")

        with pytest.raises(QuotaError):
            uploader.delete_by_record_id(collection=collection, file_data=file_data)

    def test_server_selection_timeout_raises_timeout_error(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        collection = MagicMock()
        collection.delete_many.side_effect = ServerSelectionTimeoutError("timeout")

        with pytest.raises(TimeoutError):
            uploader.delete_by_record_id(collection=collection, file_data=file_data)

    def test_auto_reconnect_raises_destination_error(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        collection = MagicMock()
        collection.delete_many.side_effect = AutoReconnect("connection lost")

        with pytest.raises(DestinationConnectionError):
            uploader.delete_by_record_id(collection=collection, file_data=file_data)

    def test_network_timeout_raises_timeout_error(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        collection = MagicMock()
        collection.delete_many.side_effect = NetworkTimeout("socket timeout during delete")

        with pytest.raises(TimeoutError):
            uploader.delete_by_record_id(collection=collection, file_data=file_data)

    def test_operation_failure_uses_structured_errmsg_for_quota(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        collection = MagicMock()
        collection.delete_many.side_effect = OperationFailure(
            "auth failed",
            code=18,
            details={
                "errmsg": "authentication failed",
                "code": 18,
                "codeName": "AuthenticationFailed",
                "ns": "quota_db.records",
            },
        )

        with pytest.raises(DestinationConnectionError):
            uploader.delete_by_record_id(collection=collection, file_data=file_data)
