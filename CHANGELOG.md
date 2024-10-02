## 0.0.22-dev3

### Enhancements

* **Add documentation for developing sources/destinations**

* **Leverage `uv` for pip compile**

* **Use incoming fsspec data to populate metadata** Rather than make additional calls to collect metadata after initial file list, use connector-specific data to populate the metadata. 

* **Astra DB V2 Source Connector** Create a v2 version of the Astra DB Source Connector.

## 0.0.21

### Fixes

* **Fix forward compatibility issues with `unstructured-client==0.26.0`.** Update syntax and create a new SDK util file for reuse in the Partitioner and Chunker

* **Update Databricks CI Test** Update to use client_id and client_secret auth. Also return files.upload method to one from open source.

* **Fix astra src bug** V1 source connector was updated to work with astrapy 1.5.0

## 0.0.20

### Enhancements

* **Support for latest AstraPy API** Add support for the modern AstraPy client interface for the Astra DB Connector.

## 0.0.19

### Fixes

* **Use validate_default to instantiate default pydantic secrets**

## 0.0.18

### Enhancements

* **Better destination precheck for blob storage** Write an empty file to the destination location when running fsspec-based precheck

## 0.0.17

### Fixes

* **Drop use of unstructued in embed** Remove remnant import from unstructured dependency in embed implementations.


## 0.0.16

### Fixes

* **Add constraint on pydantic** Make sure the version of pydantic being used with this repo pulls in the earliest version that introduces generic Secret, since this is used heavily.

## 0.0.15

### Fixes

* **Model serialization with nested models** Logic updated to properly handle serializing pydantic models that have nested configs with secret values.
* **Sharepoint permission config requirement** The sharepoint connector was expecting the permission config, even though it should have been optional.
* **Sharepoint CLI permission params made optional

### Enhancements

* **Migrate airtable connector to v2**
* **Support iteratively deleting cached content** Add a flag to delete cached content once it's no longer needed for systems that are limited in memory.

## 0.0.14

### Enhancements

* **Support async batch uploads for pinecone connector**
* **Migrate embedders** Move embedder implementations from the open source unstructured repo into this one.

### Fixes

* **Misc. Onedrive connector fixes**

## 0.0.13

### Fixes

* **Pinecone payload size fixes** Pinecone destination now has a limited set of properties it will publish as well as dynamically handles batch size to stay under 2MB pinecone payload limit.

## 0.0.12

### Enhancements

### Fixes

* **Fix invalid `replace()` calls in uncompress** - `replace()` calls meant to be on `str` versions of the path were instead called on `Path` causing errors with parameters.

## 0.0.11

### Enhancements

* **Fix OpenSearch connector** OpenSearch connector did not work when `http_auth` was not provided

## 0.0.10

### Enhancements

* "Fix tar extraction" - tar extraction function assumed archive was gzip compressed which isn't true for supported `.tar` archives. Updated to work for both compressed and uncompressed tar archives.

## 0.0.9

### Enhancements

* **Chroma dict settings should allow string inputs**
* **Move opensearch non-secret fields out of access config**
* **Support string inputs for dict type model fields** Use the `BeforeValidator` support from pydantic to map a string value to a dict if that's provided. 
* **Move opensearch non-secret fields out of access config

### Fixes

**Fix uncompress logic** Use of the uncompress process wasn't being leveraged in the pipeline correctly. Updated to use the new loca download path for where the partitioned looks for the new file.  


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

* **unstructured-client compatibility fix** Update the calls to `unstructured_client.general.partition` to avoid a breaking change in the newest version.

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
* **Autogenerate click options from base models** Leverage the pydantic base models for all configs to autogenerate the cli options exposed when running ingest as a CLI.
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
