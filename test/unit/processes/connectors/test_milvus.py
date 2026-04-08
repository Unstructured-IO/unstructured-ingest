import pytest

from unstructured_ingest.processes.connectors.milvus import (
    MilvusUploadStager,
    MilvusUploadStagerConfig,
)
from unstructured_ingest.utils.dep_check import dependency_exists

# Skip all tests if pymilvus is not available
pytestmark = pytest.mark.skipif(
    not dependency_exists("pymilvus"),
    reason="pymilvus is not installed. Install with: pip install 'unstructured-ingest[milvus]'",
)


def test_enable_lexical_search_flag_default():
    """Test that enable_lexical_search defaults to False."""
    config = MilvusUploadStagerConfig()
    assert config.enable_lexical_search is False


def test_enable_lexical_search_flag_enabled():
    """Test that enable_lexical_search can be set to True."""
    config = MilvusUploadStagerConfig(enable_lexical_search=True)
    assert config.enable_lexical_search is True


def test_create_bm25_schema_basic():
    """Test that create_bm25_schema produces correct schema structure."""
    schema, index_params = MilvusUploadStager.create_bm25_schema(
        collection_name="test_collection"
    )

    # Check schema has correct fields
    field_names = [field.name for field in schema.fields]
    assert "text" in field_names
    assert "sparse_vector" in field_names
    assert "embedding" in field_names
    assert "id" in field_names

    # Check text field has enable_analyzer
    text_field = next(f for f in schema.fields if f.name == "text")
    assert text_field.params.get("enable_analyzer") is True

    # Check sparse_vector field type
    from pymilvus import DataType
    sparse_vector_field = next(f for f in schema.fields if f.name == "sparse_vector")
    assert sparse_vector_field.dtype == DataType.SPARSE_FLOAT_VECTOR

    # Check BM25 function exists
    assert len(schema.functions) == 1
    bm25_fn = schema.functions[0]
    assert bm25_fn.name == "bm25_fn"
    assert "text" in bm25_fn.input_field_names
    assert "sparse_vector" in bm25_fn.output_field_names


def test_create_bm25_schema_custom_vector_dim():
    """Test that create_bm25_schema respects custom vector dimension."""
    schema, index_params = MilvusUploadStager.create_bm25_schema(
        collection_name="test_collection",
        vector_dim=768
    )

    embedding_field = next(f for f in schema.fields if f.name == "embedding")
    assert embedding_field.params["dim"] == 768


def test_create_bm25_schema_index_params():
    """Test that create_bm25_schema returns correct index parameters."""
    schema, index_params = MilvusUploadStager.create_bm25_schema(
        collection_name="test_collection"
    )

    assert "embedding" in index_params
    assert "sparse_vector" in index_params

    assert index_params["embedding"]["metric_type"] == "COSINE"
    assert index_params["sparse_vector"]["index_type"] == "SPARSE_INVERTED_INDEX"
    assert index_params["sparse_vector"]["metric_type"] == "BM25"


def test_create_bm25_schema_dynamic_field():
    """Test that create_bm25_schema respects enable_dynamic_field setting."""
    schema_enabled, _ = MilvusUploadStager.create_bm25_schema(
        collection_name="test_collection",
        enable_dynamic_field=True
    )
    assert schema_enabled.enable_dynamic_field is True

    schema_disabled, _ = MilvusUploadStager.create_bm25_schema(
        collection_name="test_collection",
        enable_dynamic_field=False
    )
    assert schema_disabled.enable_dynamic_field is False
