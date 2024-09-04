## 0.0.9

### Enhancements

* **Chroma dict settings should allow string inputs**
* **Move opensearch non-secret fields out of access config**
* **Support string inputs for dict type model fields** Use the `BeforeValidator` support from pydantic to map a string value to a dict if that's provided. 
* **Move opensearch non-secret fields out of access config

### Fixes

**Fix uncompress logic** Use of the uncompress process wasn't being leveraged in the pipeline correctly. Updated to use the new loca download path for where the partitioned looks for the new file.  
>>>>>>> d7a2cab (Add entry to changelog)

## 0.0.8

### Enhancements

* **Add fields_to_include option for Milvus Stager** Adds support for filtering which fields will remain in the document so user can align document structure to collection schema.
* **Add flatten_metadata option for Milvus Stager** Flattening metadata is now optional (enabled by default) step in processing the document.

## 0.0.7

### Enhancements

* **support sharing parent multiprocessing for uploaders** If an uploader needs to fan out it's process using multiprocessing, support that using the parent pipeline approach rather than handling it explicitly by the connector logic.  
* **OTEL support** If endpoint supplied, publish all traces to an otel collector. 

### Fixes

* **Weaviate access configs access** Weaviate access config uses pydantic Secret and it needs to be resolved to the secret value when being used. This was fixed. 
* **unstructured-client compatibility fix** Fix an error when accessing the fields on `PartitionParameters` in the new 0.26.0 Python client.

## 0.0.6

### Fixes

* **unstructured-client compatibility fix** Update the calls to `unstructured_client.general.partion` to avoid a breaking change in the newest version.

## 0.0.5

### Enhancements

* **Add Couchbase Source Connector** Adds support for reading artifacts from Couchbase DB for processing in unstructured
* **Drop environment from pinecone as part of v2 migration** environment is no longer required by the pinecone SDK, so that field has been removed from the ingest CLI/SDK/
* **Add KDBAI Destination Connector** Adds support for writing elements and their embeddings to KDBAI DB.

### Fixes

* **AstraDB connector configs** Configs had dataclass annotation removed since they're now pydantic data models. 
* **Local indexer recursive behavior** Local indexer was indexing directories as well as files. This was filtered out.

## 0.0.4

### Enhancements

* **Add Couchbase Destination Connector** Adds support for storing artifacts in Couchbase DB for Vector Search
* **Leverage pydantic base models** All user-supplied configs are now derived from pydantic base models to leverage better type checking and add built in support for sensitive fields.
* **Autogenerate click options from base models** Leverage th pydantic base models for all configs to autogenerate teh cli options exposed when running ingest as a CLI.
* **Drop required Unstructured dependency** Unstructured was moved to an extra dependency to only be imported when needed for functionality such as local partitioning/chunking.
* **Rebrand Astra to Astra DB** The Astra DB integration was re-branded to be consistent with DataStax standard branding.

## 0.0.3

### Enhancements

* **Improve documentation** Update the README's.
* **Explicit Opensearch classes** For the connector registry entries for opensearch, use only opensearch specific classes rather than any elasticsearch ones. 
* **Add missing fsspec destination precheck** check connection in precheck for all fsspec-based destination connectors

## 0.0.2

### Enhancements

* **Use uuid for s3 identifiers** Update unique id to use uuid derived from file path rather than the filepath itself.
* **V2 connectors precheck support** All steps in the v2 pipeline support an optional precheck call, which encompasses the previous check connection functionality. 
* **Filter Step** Support dedicated step as part of the pipeline to filter documents.

## 0.0.1

### Enhancements

### Features

* **Add Milvus destination connector** Adds support storing artifacts in Milvus vector database.

### Fixes

* **Remove old repo references** Any mention of the repo this project came from was removed. 

## 0.0.0

### Features

* **Initial Migration** Create the structure of this repo from the original code in the [Unstructured](https://github.com/Unstructured-IO/unstructured) project.

### Fixes
