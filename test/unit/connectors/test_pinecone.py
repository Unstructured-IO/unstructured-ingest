from unittest import mock

import pytest
from pydantic import ValidationError

from unstructured_ingest.errors_v2 import UserError, UserAuthError, RateLimitError, ProviderError
from unstructured_ingest.processes.connectors.pinecone import (
    PineconeAccessConfig,
    PineconeConnectionConfig,
    PineconeUploader,
    PineconeUploaderConfig,
    PineconeUploadStager,
    PineconeUploadStagerConfig,
)


@pytest.fixture
def connection_config():
    """Provides a minimal PineconeConnectionConfig for testing."""
    access_config = PineconeAccessConfig(pinecone_api_key="test-api-key")
    return PineconeConnectionConfig(
        index_name="test-index",
        access_config=access_config,
    )


@pytest.fixture
def uploader_config():
    """Provides a minimal PineconeUploaderConfig for testing."""
    return PineconeUploaderConfig(
        batch_size=100,
        pool_threads=1,
        namespace="test-namespace",
    )


@pytest.fixture
def upload_stager_config():
    """Provides a minimal PineconeUploadStagerConfig for testing."""
    return PineconeUploadStagerConfig()


def test_connection_config_valid():
    """Test that a valid connection config can be created."""
    access_config = PineconeAccessConfig(pinecone_api_key="test-key")
    config = PineconeConnectionConfig(
        index_name="test-index",
        access_config=access_config,
    )
    assert config.index_name == "test-index"
    assert config.access_config.get_secret_value().pinecone_api_key == "test-key"


def test_connection_config_no_api_key():
    """Test that connection config can be created without API key (for testing)."""
    access_config = PineconeAccessConfig()
    config = PineconeConnectionConfig(
        index_name="test-index",
        access_config=access_config,
    )
    assert config.index_name == "test-index"


def test_wrap_error_unauthorized_exception(connection_config):
    """Test that UnauthorizedException is wrapped as UserAuthError."""
    try:
        from pinecone.exceptions import UnauthorizedException
        original_error = UnauthorizedException("Invalid API key")
        wrapped_error = connection_config.wrap_error(original_error)
        
        assert isinstance(wrapped_error, UserAuthError)
        assert "Invalid API key" in str(wrapped_error)
    except ImportError:
        pytest.skip("Pinecone library not available")


def test_wrap_error_forbidden_exception(connection_config):
    """Test that ForbiddenException is wrapped as UserAuthError."""
    try:
        from pinecone.exceptions import ForbiddenException
        original_error = ForbiddenException("Access denied")
        wrapped_error = connection_config.wrap_error(original_error)
        
        assert isinstance(wrapped_error, UserAuthError)
        assert "Access denied" in str(wrapped_error)
    except ImportError:
        pytest.skip("Pinecone library not available")


def test_wrap_error_not_found_exception(connection_config):
    """Test that NotFoundException is wrapped as UserError."""
    try:
        from pinecone.exceptions import NotFoundException
        original_error = NotFoundException("Index not found")
        wrapped_error = connection_config.wrap_error(original_error)
        
        assert isinstance(wrapped_error, UserError)
        assert "Resource not found" in str(wrapped_error)
        assert "Index not found" in str(wrapped_error)
    except ImportError:
        pytest.skip("Pinecone library not available")


def test_wrap_error_pinecone_api_exception_401(connection_config):
    """Test that PineconeApiException with 401 status is wrapped as UserAuthError."""
    try:
        from pinecone.exceptions import PineconeApiException
        original_error = PineconeApiException("Unauthorized")
        original_error.status_code = 401
        wrapped_error = connection_config.wrap_error(original_error)
        
        assert isinstance(wrapped_error, UserAuthError)
        assert "Unauthorized" in str(wrapped_error)
    except ImportError:
        pytest.skip("Pinecone library not available")


def test_wrap_error_pinecone_api_exception_403(connection_config):
    """Test that PineconeApiException with 403 status is wrapped as UserAuthError."""
    try:
        from pinecone.exceptions import PineconeApiException
        original_error = PineconeApiException("Forbidden")
        original_error.status_code = 403
        wrapped_error = connection_config.wrap_error(original_error)
        
        assert isinstance(wrapped_error, UserAuthError)
        assert "Forbidden" in str(wrapped_error)
    except ImportError:
        pytest.skip("Pinecone library not available")


def test_wrap_error_pinecone_api_exception_429(connection_config):
    """Test that PineconeApiException with 429 status is wrapped as RateLimitError."""
    try:
        from pinecone.exceptions import PineconeApiException
        original_error = PineconeApiException("Rate limit exceeded")
        original_error.status_code = 429
        wrapped_error = connection_config.wrap_error(original_error)
        
        assert isinstance(wrapped_error, RateLimitError)
        assert "Rate limit exceeded" in str(wrapped_error)
    except ImportError:
        pytest.skip("Pinecone library not available")


def test_wrap_error_pinecone_api_exception_400(connection_config):
    """Test that PineconeApiException with 400 status is wrapped as UserError."""
    try:
        from pinecone.exceptions import PineconeApiException
        original_error = PineconeApiException("Bad request")
        original_error.status_code = 400
        wrapped_error = connection_config.wrap_error(original_error)
        
        assert isinstance(wrapped_error, UserError)
        assert "Bad request" in str(wrapped_error)
    except ImportError:
        pytest.skip("Pinecone library not available")


def test_wrap_error_pinecone_api_exception_500(connection_config):
    """Test that PineconeApiException with 500 status is wrapped as ProviderError."""
    try:
        from pinecone.exceptions import PineconeApiException
        original_error = PineconeApiException("Internal server error")
        original_error.status_code = 500
        wrapped_error = connection_config.wrap_error(original_error)
        
        assert isinstance(wrapped_error, ProviderError)
        assert "Internal server error" in str(wrapped_error)
    except ImportError:
        pytest.skip("Pinecone library not available")


def test_wrap_error_service_exception(connection_config):
    """Test that ServiceException is wrapped as ProviderError."""
    try:
        from pinecone.exceptions import ServiceException
        original_error = ServiceException("Service unavailable")
        wrapped_error = connection_config.wrap_error(original_error)
        
        assert isinstance(wrapped_error, ProviderError)
        assert "Service unavailable" in str(wrapped_error)
    except ImportError:
        pytest.skip("Pinecone library not available")


def test_wrap_error_pinecone_exception_rate_limit(connection_config):
    """Test that PineconeException with rate limit message is wrapped as RateLimitError."""
    try:
        from pinecone.exceptions import PineconeException
        original_error = PineconeException("Rate limit exceeded for this operation")
        wrapped_error = connection_config.wrap_error(original_error)
        
        assert isinstance(wrapped_error, RateLimitError)
        assert "Rate limit exceeded" in str(wrapped_error)
    except ImportError:
        pytest.skip("Pinecone library not available")


def test_wrap_error_pinecone_exception_auth(connection_config):
    """Test that PineconeException with auth message is wrapped as UserAuthError."""
    try:
        from pinecone.exceptions import PineconeException
        original_error = PineconeException("Authentication failed")
        wrapped_error = connection_config.wrap_error(original_error)
        
        assert isinstance(wrapped_error, UserAuthError)
        assert "Authentication failed" in str(wrapped_error)
    except ImportError:
        pytest.skip("Pinecone library not available")


def test_wrap_error_pinecone_exception_generic(connection_config):
    """Test that generic PineconeException is wrapped as UserError."""
    try:
        from pinecone.exceptions import PineconeException
        original_error = PineconeException("Some other error")
        wrapped_error = connection_config.wrap_error(original_error)
        
        assert isinstance(wrapped_error, UserError)
        assert "Some other error" in str(wrapped_error)
    except ImportError:
        pytest.skip("Pinecone library not available")


def test_wrap_error_pinecone_api_value_error(connection_config):
    """Test that PineconeApiValueError is wrapped as UserError."""
    try:
        from pinecone.exceptions import PineconeApiValueError
        original_error = PineconeApiValueError("Invalid value provided")
        wrapped_error = connection_config.wrap_error(original_error)
        
        assert isinstance(wrapped_error, UserError)
        assert "Invalid value provided" in str(wrapped_error)
    except ImportError:
        pytest.skip("Pinecone library not available")


def test_wrap_error_pinecone_api_type_error(connection_config):
    """Test that PineconeApiTypeError is wrapped as UserError."""
    try:
        from pinecone.exceptions import PineconeApiTypeError
        original_error = PineconeApiTypeError("Invalid type provided")
        wrapped_error = connection_config.wrap_error(original_error)
        
        assert isinstance(wrapped_error, UserError)
        assert "Invalid type provided" in str(wrapped_error)
    except ImportError:
        pytest.skip("Pinecone library not available")


def test_wrap_error_pinecone_configuration_error(connection_config):
    """Test that PineconeConfigurationError is wrapped as UserError."""
    try:
        from pinecone.exceptions import PineconeConfigurationError
        original_error = PineconeConfigurationError("Invalid configuration")
        wrapped_error = connection_config.wrap_error(original_error)
        
        assert isinstance(wrapped_error, UserError)
        assert "Invalid configuration" in str(wrapped_error)
    except ImportError:
        pytest.skip("Pinecone library not available")


def test_wrap_error_unhandled_exception(connection_config):
    """Test that unhandled exceptions are logged and returned as-is."""
    original_error = ValueError("Some unexpected error")
    wrapped_error = connection_config.wrap_error(original_error)
    
    assert wrapped_error is original_error
    assert isinstance(wrapped_error, ValueError)


def test_index_exists_returns_true_when_index_exists(connection_config, uploader_config):
    """Test that index_exists returns True when index exists."""
    uploader = PineconeUploader(
        connection_config=connection_config,
        upload_config=uploader_config,
    )
    
    mock_client = mock.MagicMock()
    mock_client.describe_index.return_value = {"name": "test-index"}
    
    with mock.patch.object(connection_config, 'get_client', return_value=mock_client):
        result = uploader.index_exists("test-index")
        assert result is True
        mock_client.describe_index.assert_called_once_with("test-index")


def test_index_exists_returns_false_when_index_not_found(connection_config, uploader_config):
    """Test that index_exists returns False when index doesn't exist."""
    try:
        from pinecone.exceptions import NotFoundException
        uploader = PineconeUploader(
            connection_config=connection_config,
            upload_config=uploader_config,
        )
        
        mock_client = mock.MagicMock()
        mock_client.describe_index.side_effect = NotFoundException("Index not found")
        
        with mock.patch.object(connection_config, 'get_client', return_value=mock_client):
            result = uploader.index_exists("non-existent-index")
            assert result is False
    except ImportError:
        pytest.skip("Pinecone library not available")


def test_index_exists_raises_wrapped_error_for_other_exceptions(connection_config, uploader_config):
    """Test that index_exists raises wrapped error for non-NotFound exceptions."""
    try:
        from pinecone.exceptions import UnauthorizedException
        uploader = PineconeUploader(
            connection_config=connection_config,
            upload_config=uploader_config,
        )
        
        mock_client = mock.MagicMock()
        mock_client.describe_index.side_effect = UnauthorizedException("Invalid API key")
        
        with mock.patch.object(connection_config, 'get_client', return_value=mock_client):
            with pytest.raises(UserAuthError):
                uploader.index_exists("test-index")
    except ImportError:
        pytest.skip("Pinecone library not available")


def test_precheck_raises_wrapped_error(connection_config, uploader_config):
    """Test that precheck raises wrapped error when connection fails."""
    try:
        from pinecone.exceptions import UnauthorizedException
        uploader = PineconeUploader(
            connection_config=connection_config,
            upload_config=uploader_config,
        )
        
        mock_client = mock.MagicMock()
        mock_client.describe_index.side_effect = UnauthorizedException("Invalid API key")
        
        with mock.patch.object(connection_config, 'get_client', return_value=mock_client):
            with pytest.raises(UserAuthError):
                uploader.precheck()
    except ImportError:
        pytest.skip("Pinecone library not available")


def test_format_destination_name():
    """Test that format_destination_name properly formats index names."""
    access_config = PineconeAccessConfig(pinecone_api_key="test-key")
    connection_config = PineconeConnectionConfig(
        index_name="test-index",
        access_config=access_config,
    )
    uploader_config = PineconeUploaderConfig()
    uploader = PineconeUploader(
        connection_config=connection_config,
        upload_config=uploader_config,
    )
    
    # Test basic formatting
    assert uploader.format_destination_name("Test Index") == "test-index"
    
    # Test with special characters
    assert uploader.format_destination_name("Test@Index#123") == "test-index-123"
    
    # Test with spaces and underscores
    assert uploader.format_destination_name("Test Index_123") == "test-index-123"
    
    # Test with numbers only
    assert uploader.format_destination_name("123456") == "123456"


def test_upload_stager_conform_dict(upload_stager_config):
    """Test that upload stager properly conforms dictionary."""
    from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
    
    stager = PineconeUploadStager(upload_stager_config=upload_stager_config)
    
    file_data = FileData(
        identifier="test-file-id",
        connector_type="test",
        source_identifiers=SourceIdentifiers(
            filename="test.txt",
            rel_path="test/path",
            fullpath="/full/test/path/test.txt",
        ),
    )
    
    element_dict = {
        "text": "Sample text",
        "embeddings": [0.1, 0.2, 0.3],
        "metadata": {
            "page_number": 1,
            "filename": "test.txt",
        },
        "element_id": "element-123",
    }
    
    result = stager.conform_dict(element_dict, file_data)
    
    assert "id" in result
    assert "values" in result
    assert "metadata" in result
    assert result["values"] == [0.1, 0.2, 0.3]
    assert result["metadata"]["record_id"] == "test-file-id"
    assert result["metadata"]["page_number"] == 1
    assert result["metadata"]["filename"] == "test.txt"
    assert result["metadata"]["element_id"] == "element-123"


def test_upload_stager_metadata_size_limit(upload_stager_config):
    """Test that upload stager drops metadata when it exceeds size limit."""
    from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
    
    stager = PineconeUploadStager(upload_stager_config=upload_stager_config)
    
    file_data = FileData(
        identifier="test-file-id",
        connector_type="test",
        source_identifiers=SourceIdentifiers(
            filename="test.txt",
            rel_path="test/path",
            fullpath="/full/test/path/test.txt",
        ),
    )
    
    # Create metadata that will exceed the 40KB limit
    large_metadata = {"large_field": "x" * 50000}
    
    element_dict = {
        "text": "Sample text",
        "embeddings": [0.1, 0.2, 0.3],
        "metadata": large_metadata,
    }
    
    result = stager.conform_dict(element_dict, file_data)
    
    # Fix: When metadata exceeds size limit, it should be completely dropped except for record_id
    assert result["metadata"] == {"record_id": "test-file-id"}
    assert "large_field" not in result["metadata"]
    assert "text" not in result["metadata"]  # text should also be dropped since it's not in allowed fields
