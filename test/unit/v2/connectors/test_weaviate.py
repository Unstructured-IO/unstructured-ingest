from unstructured_ingest.v2.processes.connectors.weaviate.cloud import WeaviateUploaderConfig


def test_weaviate_uploader_config_alias():
    collection = "test_collection"
    config = WeaviateUploaderConfig.model_validate(
        {
            "collection": "test_collection",
        }
    )
    assert config.collection == collection
    config = WeaviateUploaderConfig.model_validate(
        {
            "class_name": "test_collection",
        }
    )
    assert config.collection == collection
