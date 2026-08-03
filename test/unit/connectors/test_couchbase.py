import sys
from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from pydantic import Secret

# Create mock couchbase modules before importing the connector,
# since the couchbase SDK is not installed in the test environment.
mock_couchbase = MagicMock()
sys.modules.setdefault("couchbase", mock_couchbase)
sys.modules.setdefault("couchbase.auth", mock_couchbase.auth)
sys.modules.setdefault("couchbase.cluster", mock_couchbase.cluster)
sys.modules.setdefault("couchbase.options", mock_couchbase.options)

from unstructured_ingest.error import SourceConnectionError  # noqa: E402
from unstructured_ingest.processes.connectors import couchbase as couchbase_module  # noqa: E402
from unstructured_ingest.processes.connectors.couchbase import (  # noqa: E402
    CouchbaseAccessConfig,
    CouchbaseConnectionConfig,
    CouchbaseIndexer,
    CouchbaseIndexerConfig,
)

# A credential-bearing connection string of the kind a couchbase SDK error can
# embed; it must never reach the raised SourceConnectionError message.
_SECRET = "couchbase://admin:hunter2@db.internal"


@pytest.fixture
def access_config():
    return CouchbaseAccessConfig(password="testpass")


@pytest.fixture
def base_kwargs(access_config):
    return {
        "access_config": Secret(access_config),
        "username": "testuser",
        "bucket": "testbucket",
        "connection_string": "couchbase://localhost",
    }


@pytest.fixture(autouse=True)
def reset_mocks():
    """Reset all couchbase mock state between tests."""
    mock_couchbase.reset_mock()
    # Provide fresh mock instances for each test
    mock_options = MagicMock()
    mock_couchbase.options.ClusterOptions.return_value = mock_options
    mock_cluster = MagicMock()
    mock_couchbase.cluster.Cluster.connect.return_value = mock_cluster
    yield


def test_custom_timeouts_override_wan_profile(base_kwargs):
    """Custom timeout values must be applied after apply_profile so they take precedence."""
    mock_options = mock_couchbase.options.ClusterOptions.return_value

    config = CouchbaseConnectionConfig(
        **base_kwargs,
        connect_timeout_seconds=60,
        bootstrap_timeout_seconds=120,
    )

    with config.get_client():
        pass

    # apply_profile must be called
    mock_options.apply_profile.assert_called_once_with("wan_development")

    # Custom timeouts must be set via __setitem__ AFTER apply_profile.
    # Use mock_calls (not method_calls) since dunder methods are tracked there.
    all_calls = mock_options.mock_calls
    profile_call_idx = next(i for i, c in enumerate(all_calls) if "apply_profile" in str(c))
    setitem_indices = [i for i, c in enumerate(all_calls) if "__setitem__" in str(c)]

    # Timeouts set after profile
    assert len(setitem_indices) == 2
    assert all(idx > profile_call_idx for idx in setitem_indices)

    # Verify the actual timeout values
    mock_options.__setitem__.assert_any_call("connect_timeout", timedelta(seconds=60))
    mock_options.__setitem__.assert_any_call("bootstrap_timeout", timedelta(seconds=120))


def test_no_custom_timeouts_uses_profile_defaults(base_kwargs):
    """When no custom timeouts are specified, only the WAN profile defaults apply."""
    mock_options = mock_couchbase.options.ClusterOptions.return_value

    config = CouchbaseConnectionConfig(**base_kwargs)

    with config.get_client():
        pass

    mock_options.apply_profile.assert_called_once_with("wan_development")
    # No __setitem__ calls for timeouts
    mock_options.__setitem__.assert_not_called()


def test_only_connect_timeout_set(base_kwargs):
    """Setting only connect_timeout leaves bootstrap_timeout at the profile default."""
    mock_options = mock_couchbase.options.ClusterOptions.return_value

    config = CouchbaseConnectionConfig(
        **base_kwargs,
        connect_timeout_seconds=45,
    )

    with config.get_client():
        pass

    mock_options.__setitem__.assert_called_once_with("connect_timeout", timedelta(seconds=45))


def test_only_bootstrap_timeout_set(base_kwargs):
    """Setting only bootstrap_timeout leaves connect_timeout at the profile default."""
    mock_options = mock_couchbase.options.ClusterOptions.return_value

    config = CouchbaseConnectionConfig(
        **base_kwargs,
        bootstrap_timeout_seconds=90,
    )

    with config.get_client():
        pass

    mock_options.__setitem__.assert_called_once_with("bootstrap_timeout", timedelta(seconds=90))


def test_cluster_closed_on_exit(base_kwargs):
    """Cluster connection is closed when exiting the context manager."""
    mock_cluster = mock_couchbase.cluster.Cluster.connect.return_value

    config = CouchbaseConnectionConfig(**base_kwargs)

    with config.get_client():
        pass

    mock_cluster.close.assert_called_once()


def test_wait_until_ready_uses_bootstrap_timeout(base_kwargs):
    """wait_until_ready uses the configured bootstrap_timeout_seconds."""
    mock_cluster = mock_couchbase.cluster.Cluster.connect.return_value

    config = CouchbaseConnectionConfig(
        **base_kwargs,
        bootstrap_timeout_seconds=30,
    )

    with config.get_client():
        pass

    mock_cluster.wait_until_ready.assert_called_once_with(timedelta(seconds=30))


def test_wait_until_ready_default_when_no_bootstrap_timeout(base_kwargs):
    """wait_until_ready falls back to 10s when no bootstrap_timeout is configured."""
    mock_cluster = mock_couchbase.cluster.Cluster.connect.return_value

    config = CouchbaseConnectionConfig(**base_kwargs)

    with config.get_client():
        pass

    mock_cluster.wait_until_ready.assert_called_once_with(timedelta(seconds=10))


def test_get_doc_ids_error_redacts_secret(base_kwargs, monkeypatch):
    """A failing query surfaces a SourceConnectionError with no raw exception text."""
    # Avoid the real 3s-per-attempt backoff.
    monkeypatch.setattr(couchbase_module.time, "sleep", lambda *_: None)

    # get_client (fully mocked) yields this cluster; make its query() raise a
    # credential-bearing exception.
    mock_cluster = mock_couchbase.cluster.Cluster.connect.return_value
    mock_cluster.query.side_effect = Exception(f"query failed: {_SECRET}")

    config = CouchbaseConnectionConfig(**base_kwargs)
    indexer = CouchbaseIndexer(connection_config=config, index_config=CouchbaseIndexerConfig())

    with pytest.raises(SourceConnectionError) as excinfo:
        indexer._get_doc_ids()

    message = str(excinfo.value)
    assert _SECRET not in message
    assert "hunter2" not in message
    # The non-secret free text of the exception is stripped too, not just the
    # known credential -- only the exception type survives.
    assert "query failed" not in message
    # The sanitized summary still names the exception type for troubleshooting.
    assert "Exception" in message
