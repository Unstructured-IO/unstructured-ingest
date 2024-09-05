from unstructured_ingest.v2.processes.connectors import opensearch


def test_instantiate_opensearch_connector_client():
    uploader_config = opensearch.OpenSearchUploaderConfig(index_name="dest")
    connection_config = opensearch.OpenSearchConnectionConfig(
        access_config=opensearch.OpenSearchAccessConfig()
    )
    uploader = opensearch.OpenSearchUploader(
        upload_config=uploader_config, connection_config=connection_config
    )
    uploader.connection_config.get_client()
