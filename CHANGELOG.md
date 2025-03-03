## 0.5.10-dev0

### Enhancements

* **Migrate Jira Source connector from V1 to V2**
* **Add Jira Source connector integration and unit tests**
* **Added option for custom OpenAI baseurl in EmbedderConfig**

## 0.5.9

### Features

* Add auto create collection support for AstraDB destination

### Fixes

* **Fix Confluence Source page title not being processed during partition**

## 0.5.8

### Fixes

* **Fix on pinecone index creation functionality**

## 0.5.7

### Fixes

* **Fix voyageai embedder: add multimodal embedder function**

## 0.5.6

### Enhancements

* **Add support for setting up destination for Pinecone**
* Add name formatting to Weaviate destination uploader

## 0.5.5

### Enhancements

* **Improve orig_elements handling in astra and neo4j connectors**

## 0.5.4

### Enhancements

* **Sharepoint support for nested folders and remove need for default path Shared Documents**

## 0.5.3

### Enhancements

* **Improvements on Neo4J uploader, and ability to create a vector index**
* **Optimize embedder code** - Move duplicate code to base interface, exit early if no elements have text. 

### Fixes

* **Fix bedrock embedder: rename embed_model_name to embedder_model_name**

## 0.5.2

### Enhancements

* **Improved google drive precheck mechanism**
* **Added integration tests for google drive precheck and connector**
* **Only embed elements with text** - Only embed elements with text to avoid errors from embedders and optimize calls to APIs.
* **Improved google drive precheck mechanism**
* **Added integration tests for google drive precheck and connector**

### Fixes

* **Fix Snowflake Uploader error with array variable binding**

## 0.5.1

### Fixes

* **Fix Attribute Not Exist bug in GoogleDrive connector**
* **Fix query syntax error in MotherDuck uploader**
* **Fix missing output filename suffix in DuckDB base stager**

### Enhancements

* **Allow dynamic metadata for SQL Connectors**
* **Add entities field to pinecone connector default fields**

## 0.5.0

### Fixes

* **Change aws-bedrock to bedrock**
* **Update Sharepoint tests**

### Enhancements

* **Don't raise error by default for unsupported filetypes in partitioner** - Add a flag to the partitioner to not raise an error when an unsupported filetype is encountered.

## 0.4.7

### Fixes

* **Add missing async azure openai embedder implementation**
* **Update Sharepoint to support new Microsoft credential sequence**

## 0.4.6

### Fixes

* **Fix Upload support for OneDrive connector**
* **Fix Databricks Delta Tables connector's "Service Principal" authentication method**

## 0.4.5

### Fixes

* **Fix downloading large files for OneDrive**

## 0.4.4

### Fixes

* **Fix AsyncIO support for OneDrive connector**

## 0.4.3

### Enhancements

* **Add support for allow list when downloading from raw html**
* **Add support for setting up destination as part of uploader**
* **Add batch support for all embedders**

### Fixes

* **Fix HtmlMixin error when saving downloaded files**
* **Fix Confluence Downloader error when downloading embedded files**

## 0.4.2

### Fixes

* **Fix Databricks Volume Delta Table uploader** - Use given database when uploading data.

## 0.4.1

### Enhancements

* **Support img base64 in html**
* **Fsspec support for direct URI**
* **Support href extraction to local file**
* **Added VastDB source and destination connector**

### Fixes

* **Fix how data updated before writing to sql tables based on columns in table**

## 0.4.0

### Enhancements

* **Change Confluence Source Connector authentication parameters to support password, api token, pat token and cloud authentication**

### Fixes

* **Fix SQL uploader stager** - When passed `output_filename` without a suffix it resulted in unsupported file format error. Now, it will take a suffix of `elements_filepath` and append it to `output_filename`.
* **Fix Snowflake uploader** - Unexpected `columns` argument was passed to `_fit_to_schema` method inside SnowflakeUploader `upload_dataframe` method.

## 0.3.15

### Enhancements

* **Add databricks delta table connector**

### Fixes

* **Fixed namespace issue with pinecone, and added new test**

## 0.3.14

### Fixes

* **Fix Neo4j Uploader string enum error**
* **Fix ChromaDB Destination failing integration tests** - issue lies within the newest ChromaDB release, fix freezes it's version to 0.6.2.

## 0.3.13

### Fixes

* **Fix Snowflake Uploader error**
* **Fix SQL Uploader Stager timestamp error**
* **Migrate Discord Sourced Connector to v2**
* **Add read data fallback** When reading data that could be json or ndjson, if extension is missing, fallback to trying to read it as json.

### Enhancements

* **Async support for all IO-bounded embedders**
* **Expand support to Python 3.13**

## 0.3.12

### Enhancements

* **Migrate Notion Source Connector to V2**
* **Migrate Vectara Destination Connector to v2**
* **Added Redis destination connector**
* **Improved Milvus error handling**
* **Bypass asyncio exception grouping to return more meaningful errors from OneDrive indexer**
* **Kafka destination connector checks for existence of topic**
* **Create more reflective custom errors** Provide errors to indicate if the error was due to something user provided or due to a provider issue, applicable to all steps in the pipeline.

### Fixes
* **Register Neo4j Upload Stager**
* **Fix Kafka destination connection problems**


## 0.3.11

### Enhancements

* **Support Databricks personal access token**

### Fixes

* **Fix missing source identifiers in some downloaders**

## 0.3.10

### Enhancements

* **Support more concrete FileData content for batch support**

### Fixes

* **Add Neo4J to ingest destination connector registry**
* **Fix closing SSHClient in sftp connector**

## 0.3.9

### Enhancements

* **Support ndjson files in stagers**
* **Add Neo4j destination connector**
* **Support passing data in for uploaders**

### Fixes

* **Make sure any SDK clients that support closing get called**

## 0.3.8

### Fixes

* **Prevent pinecone delete from hammering database when deleting**

## 0.3.7

### Fixes

* **Correct fsspec connectors date metadata field types** - sftp, azure, box and gcs
* **Fix Kafka source connection problems**
* **Fix Azure AI Search session handling**
* **Fixes issue with SingleStore Source Connector not being available**
* **Fixes issue with SQLite Source Connector using wrong Indexer** - Caused indexer config parameter error when trying to use SQLite Source
* **Fixes issue with Snowflake Destination Connector `nan` values** - `nan` values were not properly replaced with `None`
* **Fixes Snowflake source `'SnowflakeCursor' object has no attribute 'mogrify'` error**
* **Box source connector can now use raw JSON as access token instead of file path to JSON**
* **Fix fsspec upload paths to be OS independent**
* **Properly log elasticsearch upload errors**

### Enhancements

* **Kafka source connector has new field: group_id**
* **Support personal access token for confluence auth**
* **Leverage deterministic id for uploaded content**
* **Makes multiple SQL connectors (Snowflake, SingleStore, SQLite) more robust against SQL injection.**
* **Optimizes memory usage of Snowflake Destination Connector.**
* **Added Qdrant Cloud integration test**
* **Add DuckDB destination connector** Adds support storing artifacts in a local DuckDB database.
* **Add MotherDuck destination connector** Adds support storing artifacts in MotherDuck database.
* **Update weaviate v2 example**

## 0.3.6

### Fixes

* **Fix Azure AI Search Error handling**

## 0.3.5

### Enhancements

* **Persist record id in dedicated LanceDB column, use it to delete previous content to prevent duplicates.**

### Fixes

* **Remove client.ping() from the Elasticsearch precheck.**
* **Pinecone metadata fixes** - Fix CLI's --metadata-fields default. Always preserve record ID tracking metadata.
* **Add check to prevent querying for more than pinecone limit when deleting records**
* **Unregister Weaviate base classes** - Weaviate base classes shouldn't be registered as they are abstract and cannot be instantiated as a configuration

## 0.3.4

### Enhancements

* **Add azure openai embedder**
* **Add `collection_id` field to Couchbase `downloader_config`**

## 0.3.3

### Enhancements

* **Add `precheck` to Milvus connector**

### Fixes

* **Make AstraDB uploader truncate `text` and `text_as_html` content to max 8000 bytes**
* **Add missing LanceDb extra**
* **Weaviate cloud auth detection fixed**

## 0.3.2

### Enhancements

* **Persist record id in mongodb data, use it to delete previous content to prevent duplicates.**


### Fixes

* **Remove forward slash from Google Drive relative path field**
* **Create LanceDB test databases in unique remote locations to avoid conflicts**
* **Add weaviate to destination registry**

## 0.3.1

### Enhancements

* **LanceDB V2 Destination Connector**
* **Persist record id in milvus, use it to delete previous content to prevent duplicates.**
* **Persist record id in weaviate metadata, use it to delete previous content to prevent duplicates.**
* **Persist record id in sql metadata, use it to delete previous content to prevent duplicates.**
* **Persist record id in elasticsearch/opensearch metadata, use it to delete previous content to prevent duplicates.**

### Fixes

* **Make AstraDB precheck fail on non-existant collections**
* **Respect Pinecone's metadata size limits** crop metadata sent to Pinecone's to fit inside its limits, to avoid error responses
* **Propagate exceptions raised by delta table connector during write**

## 0.3.0

### Enhancements

* **Added V2 kafka destination connector**
* **Persist record id in pinecone metadata, use it to delete previous content to prevent duplicates.**
* **Persist record id in azure ai search, use it to delete previous content to prevent duplicates.**
* **Persist record id in astradb, use it to delete previous content to prevent duplicates.**
* **Update Azure Cognitive Search to Azure AI Search**

### Fixes

* **Fix Delta Table destination precheck** Validate AWS Region in precheck.
* **Add missing batch label to FileData where applicable**
* **Handle fsspec download file into directory** When filenames have odd characters, files are downloaded into a directory. Code added to shift it around to match expected behavior.
* **Postgres Connector Query** causing syntax error when ID column contains strings

## 0.2.2

### Enhancements
* **Remove `overwrite` field** from fsspec and databricks connectors
* **Added migration for GitLab Source V2**
* **Added V2 confluence source connector**
* **Added OneDrive destination connector**
* **Qdrant destination to v2**
* **Migrate Kafka Source Connector to V2**

## 0.2.1

### Enhancements

* **File system based indexers return a record display name**
* **Add singlestore source connector**
* **Astra DB V2 Source Connector** Create a v2 version of the Astra DB Source Connector.
* **Support native async requests from unstructured-client**
* **Support filtering element types in partitioner step**


### Fixes

* **Fix Databricks Volumes file naming** Add .json to end of upload file.
* **Fix SQL Type destination precheck** Change to context manager "with".

## 0.2.0

### Enhancements

* **Add snowflake source and destination connectors**
* **Migrate Slack Source Connector to V2**
* **Migrate Slack Source Connector to V2**
* **Add Delta Table destination to v2**
* **Migrate Slack Source Connector to V2**

## 0.1.1

### Enhancements

* **Update KDB.AI vectorstore integration to 1.4**
* **Add sqlite and postgres source connectors**
* **Add sampling functionality for indexers in fsspec connectors**

### Fixes

* **Fix Databricks Volumes destination** Fix for filenames to not be hashes.

## 0.1.0

### Enhancements

* **Move default API URL parameter value to serverless API**
* **Add check that access config always wrapped in Secret**
* **Add togetherai embedder support**
* **Refactor sqlite and postgres to be distinct connectors to support better input validation**
* **Added MongoDB source V2 connector**
* **Support optional access configs on connection configs**
* **Refactor databricks into distinct connectors based on auth type**

### Fixes

**Fix Notion Ingestion** Fix the Notion source connector to work with the latest version of the Notion API (added `in_trash` properties to `Page`, `Block` and `Database`).

## 0.0.25

### Enhancements

* **Support pinecone namespace on upload**
* **Migrate Outlook Source Connector to V2**
* **Support for Databricks Volumes source connector**

### Fixes

* **Update Sharepoint Creds and Expected docs**

## 0.0.24

### Enhancements

* **Support dynamic metadata mapping in Pinecone uploader**

## 0.0.23

### Fixes

* **Remove check for langchain dependency in embedders**

## 0.0.22

### Enhancements

* **Add documentation for developing sources/destinations**

* **Leverage `uv` for pip compile**

* **Use incoming fsspec data to populate metadata** Rather than make additional calls to collect metadata after initial file list, use connector-specific data to populate the metadata.

* **Drop langchain as dependency for embedders**

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
