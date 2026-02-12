## [1.4.1]

* **fix: AstraDB collection errors should return 400 status code**

## [1.4.0]

* **feat: add Python 3.11 and 3.13 support** Widen `requires-python` from `>=3.12, <3.13` to `>=3.11, <3.14`. Add Python 3.11 and 3.13 classifiers. Expand CI version matrices for lint and test jobs to cover all three versions.
* **chore: update CI runners** Use `opensource-linux-8core` for test jobs that benefit from parallelism (`test_ingest_unit`, `test_ingest_unit_unstructured`, integration tests, E2E tests); keep `ubuntu-latest` for lightweight jobs (lint, shellcheck, release, etc.).
* **fix: e2e `src_api_test` missing matrix** Hardcode `python-version: "3.12"` for the `src_api_test` job which previously referenced an undefined `matrix.python-version`.
* **chore: add workflow permissions** Add explicit least-privilege `permissions` blocks to `unit_tests.yml`, `e2e.yml`, and `release.yml`.
* **fix: pin ruff via uv override** Override `clarifai`'s hard pin on `ruff==0.11.4` and `psutil==7.0.0` so the lockfile resolves to latest versions.

## [1.3.3]

* **fix: use couchbase constructor that respects timeout configurations**

## [1.3.2]

* **feat: add oauth token option to google drive**

## [1.3.1]

* **fix: improve pinecone connector error handling by passing through the status code**

## [1.3.0]

* **chore: migrate to native uv dependency management** Inline all dependencies from `requirements/*.txt` into `pyproject.toml`, remove `hatch-requirements-txt` plugin, and delete the `requirements/` directory and `scripts/pip-compile.sh`.
* **chore: clean up dependencies** Bump `certifi>=2026.1.4`, fix `bs4` to `beautifulsoup4`, fix `pytest_tagging` to `pytest-tagging`. Remove stale CI constraint group (deltalake, tritonclient, numpy, fsspec, etc. pins).
* **chore: drop Python 3.10/3.11 support** Require `>=3.12, <3.13`. Remove `pandas<3` pin (no longer needed with 3.12+). Remove dead `msg` local partition extra (not defined by upstream `unstructured` package).
* **chore: update Makefile** Add `make lock` (`uv lock --upgrade`), add `make install` and `make tidy` shortcuts, rename test targets to `test-unit`/`test-integration` convention, add `--frozen --no-sync` to all `uv run` commands.
* **chore: add pytest-xdist** Parallelize unit tests with `-n auto`.
* **chore: update CI workflows** Fix Python version defaults to 3.12, add `.python-version` file, fix `setup-python` action to use `inputs.python-version`, fix fixture update workflow (branch naming, docker cleanup, permissions, `GITHUB_TOKEN`).
* **chore: bump Milvus docker-compose image** `v2.3.19` to `v2.5.26` for integration tests.
* **chore: exclude `multi_page_image.tif` from partitioner integration tests** Too large for hi_res API processing within CI timeout limits.
* **chore: regenerate stale e2e test fixtures** Update expected output for s3, azure, biomed, and google-drive connectors.
* **fix: PydanticDeprecatedSince211 warning** Extract `_is_optional_field` helper using `typing.get_origin`/`get_args` instead of raw dunder access on `model_fields`.
* **fix: astradb unit test** Fix `with_options` mock to use sync `MagicMock` instead of inheriting `AsyncMock`.
* **fix: test collection** Add `conftest.py` to `test/unit/unstructured/` to skip collection when `unstructured` package is not installed.
* **fix: databricks-volumes upload parameter** Revert `content` back to `contents` to match `databricks-sdk>=0.85.0`.
* **fix: notion connector for notion-client 2.7.0** Replace `super().query()` calls with direct `self.parent.request()` calls, since `DatabasesEndpoint.query()` was removed upstream.
* **fix: S3 special character test** Exclude `additional_metadata.ChecksumType` from comparison (new AWS S3 metadata field).

## [1.2.40]

* **fix: prevent filename corruption when file/folder names contain the bucket or path prefix**

## [1.2.39]

* **fix: fix leading slash convention for sftp paths**

## [1.2.38]

* **fix: remove sftp indexer run override**

## [1.2.37]

* **fix: sftp protocol needs default**

## [1.2.36]

* **fix: allow sftp via paramiko for kubernetes**

## [1.2.35]

* **fix: improve sftp connector**

## [1.2.34]

* **fix: remove teradata charset setting**

## [1.2.33]

* **fix: update Vectara token endpoint**

## [1.2.32]

* **feat: add teradata source and destination**

## [1.2.31]

* **fix: Add concurrency limit to AstraDB uploader to prevent timeouts**

## [1.2.30]

* **fix: Upgrade langchain-core to resolve critical vulnerability CVE-2025-68664**

## [1.2.29]

* **fix: add simple retry and timeout logic to Opensearch connector**

## [1.2.28]

* **fix: Limit opensearch-py to below 3.0.0 to prevent grpcio conflicts with milvus and watsonx**

## [1.2.27]

* **feat: Add AWS IAM authentication and full async support to OpenSearch connector**

## [1.2.26]

* **feat: add astra_generated_embeddings and enable_lexical_search options to AstraDB stager/uploader**

## [1.2.25]

* **fix: dont submit fields for Elasticsearch/Opensearch if there are none specified**

## [1.2.24]

* **fix: Remove --skip-existing from release.yml**

## [1.2.23]

* **fix: Improve opensearch with correct connector_type metadata and compatible index validation**

## [1.2.22]

* **feat: Add option to disable binary encoded vectors in AstraDBUploader**

## [1.2.21]

* **fix: Enforce minimum version of databricks-sdk (>=0.62.0) for databricks-volumes connector**
* **fix: Update databricks-volumes connector to wrap file in io.BytesIO for BinaryIO compatibility** *(Note: the original entry incorrectly stated a rename to `content`; the SDK parameter is `contents`. Reverted in 1.3.0.)*
* **fix: Add constraints to prevent platform compatibility issues with tritonclient/perf-analyzer dependencies**

## [1.2.20]

* **fix: Prevent weaviate cloud precheck from passing with invalid config**

## [1.2.19]

* **fix: Pinned aibotocore to skip version that's incompatible with recent botocore version**

## [1.2.18]

* **feat: add configurable Bedrock inference profile support**

## [1.2.17]

* **Enhancement: Use a single `executemany` instead of per element `execute` in Snowflake Uploader**

## [1.2.16]

* **Fix: Catch databricks client auth errors that were being missed**

## [1.2.15]

* **Fix: Filter out fields that aren't part of our Page subclass data model. This guards against API changes that are potentially nonbreaking.**

## [1.2.14]

* **Fix: IBM watsonx.data S3 bucket authentication fix**

## [1.2.13]

* **Feat: Make Bedrock embedding credentials optional and add IAM support**
  - AWS credentials (`aws_access_key_id`, `aws_secret_access_key`) are now optional, defaulting to `None`
  - Added `access_method` field supporting "credentials" (explicit keys) and "iam" (AWS credential chain)
  - Added `endpoint_url` field for custom Bedrock endpoints
  - Enhanced validation logic for different authentication methods
  - Maintains full backwards compatibility with existing configurations

## [1.2.12]

* **Fix: retry with wait when throttling error happens in Sharepoint connector**
* **Fix: fix Milvus stager to use correct exception**
* **Fix: confluence integration test to use new link and credential**

## [1.2.11]

* **Fix: temporarily restore errors_v2.py**

## [1.2.10]

* **o11y: standardize exception classes across the repo**:

## [1.2.9]

**Fix: enable s3fs cache_regions for bucket region detection**

## [1.2.8]

**Fix: Fix artifact url**

## [1.2.7]

**Fix: Install extras that use requirements files**

## [1.2.6]

**Fix: Fix requirements issue with Weaviate uploader**

## [1.2.5]

**Fix: Fix requirements issue with Weaviate uploader**

## [1.2.4]

**Fix: Fix requirements issue with weaviate uploader**

## [1.2.3]

**Fix: Update chroma version for Python 3.12 compatibility**

## [1.2.2]

**Fix: Pin pydantic to v2.9.1 for compatibility**

## [1.2.1]

**Fix: Pin pydantic to v2.8.2 for compatibility**

## [1.2.0]

**feat: add Milvus connector**

## [1.1.3]

**Fix: `__version__.py` regex pattern was broken**

## [1.1.2]

**Fix: Fix logger**

## [1.1.1]

**Fix: Fix requirements**

## [1.1.0]

**Feat: add vertex ai embedding support**

## [1.0.59]

**Fix: Box connector configuration compatibility**

## [1.0.58]

**Fix: Box connector configuration compatibility**

## [1.0.57]

**Fix: Box connector configuration compatibility**

## [1.0.56]

**Fix: Box connector configuration compatibility**

## [1.0.55]

**Fix: Fix box uploader path construction**

## [1.0.54]

**Fix: Databricks file system setup**

## [1.0.53]

**Feat: Add retry for Sharepoint connector connection errors**

## [1.0.52]

**Fix: Fix box uploader**

## [1.0.51]

**Fix: Update Databricks SDK version and SQL warehouse types**

## [1.0.50-dev0]

**Feat: Add Databricks Delta Lake Table connector**

## [1.0.49]

**Fix: Azure file download**

## [1.0.48]

**Fix: Azure file download**

## [1.0.47]

**Fix: Azure file download**

## [1.0.46]

**Feat: Support subdirectories in Onedrive**

## [1.0.45]

* **Feat: Support subdirectories for Google Drive connector**

## [1.0.44]

**Fix: Support subdirectories in Onedrive**

## [1.0.43]

**Fix: better error handling**

## [1.0.42]

**Feat: Increase robustness of BigQuery connector**

## [1.0.41]

**Feat: Update elasticsearch version range**

## [1.0.40]

**Fix: Use pydantic core for validation of weaviate dict types**

## [1.0.39]

**Feat: add supabase vector db support**

## [1.0.38]

**Feat: add batch and retry mechanism for weaviate uploader**

## [1.0.37]

**Fix: Remove use of deprecated python dateutil class**

## [1.0.36]

**Fix: Set default parallelism on Azure upload**

## [1.0.35]

**Fix: S3, Azure, GCS uploaders to parallelize uploads **

## [1.0.34]

**Fix: fix Jira bulk_size field name**

## [1.0.33]

**Feat: Add Jira connector**

## [1.0.32]

**Feat: add dropbox connector support**

## [1.0.31]

**Feat: Qdrant VectorDB Connector Support **

## [1.0.30]

* **Feat: Delta table connector functionality**

## [1.0.29]

* **Add Sharepoint connector **
* **Add Onedrive connector**

## [1.0.28]

* **Fix: allow setting metadata and set metadata for mongodb stager**
* **Fix: check page content for Notion**

## [1.0.27]

* **Fix: Fix notion source script **
* **Fix: Set min version for sqlalchemy**
* **Fix: Fix elasticsearch params**

## [1.0.26]

**Fix: Fix dependency issue that prevents discord module from loading**

## [1.0.25]

**Fix: Fix upload logic for BigQuery**

## [1.0.24]

**Fix: Check if columns exist in MongoDB**

## [1.0.23]

* **Fix: Fix snowflake extra dependency**

## [1.0.22]

**Fix: fsspec path compatibility for GCS**

## [1.0.21]

* **Feat: Add embedding support to the ingest framework**

## [1.0.19]

**Fix: Allow passing partition_by_api as part of ingest**

## [1.0.18]

**Fix: Fix compatibility with the main lib **

## [1.0.17]

**Fix: Pass kwargs through for databricks volume connector**

## [1.0.16]

**Fix: Fallback to SQL database functionality for databricks if volume functionality doesn't work**

## [1.0.15]

* **Enhancement: Add Databricks volume and file index support**
* **Enhancement: Add Weaviate support**

## [1.0.14]

* **Fix: fix the regex for Azure Cognitive Search uploader to support the cognitive search endpoint URL**
* **Fix: set the metadata for the list_elements function of the Astra database**

## [1.0.13]

* **Fix: change the regex for Azure Cognitive Search uploader to support new-style URLs**
* **Fix: Azure Cognitive Search uploader to support different data types**
* **Enhancement: add additional metadata to upload info for MongoDB**

## [1.0.12]

* **Fix: write stage in partition dir when remote url ends with /**
* **Fix: remove empty directories for remote storage systems**
* **Fix: Add timeout to google drive source and fix the dependencies**

## [1.0.11]

* **Enhancement: Add Slack Connector**
* **Enhancement: Add KnowledgeGraph Reader**
* **Fix: handle exceptions during file ingest**
* **Fix: fix small multithread issue**

## [1.0.8]

* **Enhancement: new Databricks connection using SQL warehouse**
* **Fix: Bug fixes and general stability improvement**

## [1.0.7]

* **Enhancement: Add Astra database connector**
* **Fix: update the embedding model name**

## [1.0.6]

* **Enhancement: add OpenAI embedding api key as secrets string**
* **Enhancement: Implement Zendesk Source Connector**
* **Enhancement: Improvements to elasticsearch source connector**
* **Fix: Enhance postgres connector to support 3rd part databases**

## [1.0.5]

* **Enhancement: Postgres source connector to use query**
* **Enhancement: Adds source connector for Box**

## [1.0.4]

* **Enhancement: Enhanced source connector for BigQuery**
* **Enhancement: Kafka source and destination connectors**

## [1.0.3]

* **Enhancement: Gitlab source connector**
* **Enhancement: Elasticsearch source connector**

## [1.0.2]

* **Enhancement: Improvements to Reddit source connector**
* **Enhancement: Add uploader for SQL databases**

## [1.0.1]

* **Enhancement: Reddit source connector**
* **Enhancement: Opensearch source connector**
* **Fix: Azure upload using ADLS gen2 client**

## [0.7.2]

* **Enhancement: BigQuery source connector**
* **Fix: Azure file identification bug fix**

## [0.7.0]

* **Enhancement: Azure Cognitive Search destination connector**
* **Fix: Google Drive source connector dependency fix**

## [0.6.4]

* **Enhancement: improvements to Google Drive source connector**
* **Enhancement: improvements to Notion source connector**
* **Enhancement: MongoDB source and destination connectors**
* **Enhancement: Google Cloud Storage and Azure file stream support**
* **Enhancement: Postgres destination connector**

## [0.6.3]

* **Enhancement: Biomed source connector**
* **Enhancement: Single-threaded processing option**

## [0.6.2]

* **Enhancement: Google Drive source connector**
* **Enhancement: Improved error messaging and logging**
* **Enhancement: Fsspec implementations for major cloud storage destinations**

## [0.6.1]

* **Enhancement: Notion source connector**
* **Enhancement: Destination connectors for cloud file systems**

## [0.6.0]

* **Enhancement: Google Cloud Storage source connector**
* **Enhancement: Azure source connector**

## [0.5.25]

* **Enhancement: Email source connector**
* **Enhancement: Hubspot source connector**

## [0.5.24]

* **Enhancement: Webhook destination connector**
* **Enhancement: improvements to Discord source connector**

## [0.5.23]

* **Enhancement: Discord source connector**
* **Enhancement: Wikipedia source connector**

## [0.5.22]

* **Enhancement: Airtable source connector**
* **Enhancement: Delta Table source connector**

## [0.5.21]

* **Enhancement: Source connector for RSS**
* **Enhancement: Destination connector for DynamoDB**

## [0.5.20]

* **Enhancement: Local source connector can now handle .msg files**
* **Enhancement: Confluence source connector**

## [0.5.19]

* **Enhancement: Source connector for S3**
* **Enhancement: Destination connector for SQL databases**

## [0.5.18]

* **Enhancement: Source connector for SharePoint**
* **Enhancement: Source connector for Outlook**

## [0.5.17]

* **Enhancement: Source connector for Google Drive**

## [0.5.16]

* **Enhancement: Destination connector for Chroma**
* **Enhancement: Destination connector for Elasticsearch**

## [0.5.15]

* **Enhancement: Source connector for Local Filesystem**
* **Enhancement: Destination connector for Local Filesystem**
* **Enhancement: Miscellaneous stability improvements**
* **Enhancement: Source connector for Sftp**

## [0.5.14]

* **Enhancement: Source connector for Oracle**
* **Enhancement: Destination connector for Pinecone**

## [0.5.13]

* **Enhancement: Source connector for SQL**
* **Enhancement: Source connector for Azure**

## [0.5.12]

* **Enhancement: Source connector for S3**
* **Enhancement: Destination connector for Azure**
* **Enhancement: Destination connector for S3**

## [0.5.11]

* **Enhancement: Source and destination connectors for Google Cloud Storage**
* **Enhancement: Improvements to all destination connectors**

## [0.5.10]

* **Enhancement: Add destination connectors for Weaviate**
* **Enhancement: Add destination connectors for Opensearch**
* **Enhancement: Add destination connectors for Databricks**
* **Enhancement: Expanded source connector for Notion**
* **Enhancement: Expanded source connector for Confluence**
* **Enhancement: Allow multipart upload strategy for S3**

## [0.5.9]

* **Enhancement: Add source connector for S3**
* **Enhancement: Support large PDF documents (> 25MB) for remote file systems**

## [0.5.8]

* **Enhancement: Add source connector for Github**

## [0.5.7]

* **Enhancement: Initial fsspec destination connectors**

## [0.5.6]

* **Enhancement: Initial source connectors**

## [0.5.5]

**bugfix: move into pyproject.toml**

## [0.5.4]

* **Enhancement: Fix version management**

## [0.5.3]

* **Enhancement: Combine the upstream table table sources**
* **Enhancement: Add a new way to upload unstructured table outputs**
* **Enhancement: Process metadata table with the same upload strategy used for structured outputs**
* **bugfix: Enable setting the database name for sql destination connector**
* **bugfix: Enable setting the schema name for postgresql destination connector**

## [0.5.2]

* **Enhancement: add a data mapper for delta table stager**
* **Enhancement: combine the text table sources and make it async compatible**
* **Enhancement: enable setting the table name for sql destination connector**
* **Enhancement: set timeout default to 30 seconds for sharepoint connector**
* **Enhancement: support for `batch_size` table partitioning**

## [0.5.1]

* **Enhancement: Support for S3 bucket location endpoint URLs**
* **Enhancement: Upgrade dependency requirements**
* **Enhancement: Support for local connector's configuration file**
* **Enhancement: Support csv files as structured data for postgres connector**
* **Enhancement: Connection type tracking support for delta table destination connector**
* **Enhancement: Stream for unstructured elements as structured data for delta table connector**

## [0.5.0]

* **Enhancement: Support for config files (in YAML or TOML)**
* **Enhancement: Chroma connector supports now async**

## [0.4.7]

* **Enhancement: Azure destination support**
* **Enhancement: gcs-utils to create large blob storage support**

## [0.4.6]

* **Enhancement: Notion destination connector**
* **Enhancement: Postgres destination connector**

## [0.4.5]

* **Enhancement: Upload strategy**

## [0.4.4]

* **Enhancement: Download path tracking**

## [0.4.3]

* **Enhancement: Add Weaviate support**
* **bugfix: Fix Github connector to include .txt file extension**
* **bugfix: Fix download dir for processors**
* **Enhancement: Add Azure Cognitive Search support**
* **Enhancement: Add Chroma support**
* **Enhancement: Add Databricks support**
* **Enhancement: Add fsspec for S3 connector**

## [0.4.2]

* **Enhancement: SFTP recursive processing**

## [0.4.1]

* **Enhancement: SQL connector enhancements**
* **Enhancement: Replace Elasticsearch-DSL library with official Elasticsearch package**
* **Enhancement: Support for shared folder access in SharePoint**
* **Enhancement: Dedupe command line argument for connectors to use when upstreaming**
* **Enhancement: Add `exclude-metadata` option to reduce package size when uploading to destinations**

## [0.4.0]

* **Enhancement: Adds support for Elasticsearch 8.x**
* **Enhancement: Support for Outlook to work with .msg files**
* **Enhancement: Local connector partitioning by file size**
* **Enhancement: Using partition_strategy from CLI for all processed files**

## [0.3.15]

* **Enhancement: Adds notion config files for user and database IDs for each document**
* **Enhancement: Salesforce connector to handle different datasets**
* **Enhancement: Confluence connector timeout and error handling**

## [0.3.14]

* **Enhancement: Optimize Notion connector performance**
* **Enhancement: Notion connector metadata**

## [0.3.13]

* **Enhancement: Add Notion connector**
* **Enhancement: Format and style improvements to sql connector**
* **Enhancement: Multi-threaded file upload for S3**
* **Enhancement: Support for newer versions of PostgreSQL**
* **Enhancement: Notion connector filter for empty pages**
* **Enhancement: Add support for additional Notion document types (callout, bulleted list)**
* **Enhancement: Pass partition strategy to files from local connector when uploading to destinations**

## [0.3.12]

* **Enhancement: Performance improvement to locally-sourced connectors**
* **Enhancement: Add Oracle connector**
* **Enhancement: Support for Pinecone serverless indexes**
* **Enhancement: Add Slack connector**
* **Enhancement: Add support for Outlook .msg file format**
* **Enhancement: Add sql connector**
* **Enhancement: Add Dropbox connector**
* **Enhancement: Add OracleDB destination connector**
* **Enhancement: Add Sharepoint connector**
* **Enhancement: Add Azure connector**

## [0.3.11]

* **Enhancement: Google Drive connector to handle large files**
* **Enhancement: PostgreSQL destination connector**

## [0.3.10]

* **Enhancement: Confluence connector to use published_date to check for freshness**
* **Enhancement: S3 connector to accept kwargs for client creation**
* **Enhancement: Add Pinecone destination connector**

## [0.3.9]

* **Enhancement: Google Cloud Platform S3-compatible connector**
* **Enhancement: Support custom CA bundles**
* **Enhancement: GoogleDrive connector to handle different formats including PDFs**
* **Enhancement: Add support for .txt files to the Gitlab connector**

## [0.3.8]

* **Enhancement: Add Github connector**

## [0.3.7]

* **Enhancement: Add Box connector**
* **Enhancement: Add Gitlab connector**
* **Enhancement: Add Wikipedia connector**
* **Enhancement: Add RSS connector**
* **Enhancement: Confluence connector can now query in CQL or use labels to filter content.**
* **Enhancement: Add email connector that support .eml files**
* **Enhancement: Add S3 compatible object storage systems like Minio**
* **Enhancement: Outlook connector supports server connection**
* **Enhancement: Add Google Drive connector**
* **Enhancement: Enable single-threaded processing mode**
* **Enhancement: Connect to Opensearch through AWS Signatures**
* **Enhancement: Support downloading only unknown documents from cloud sources**
* **Enhancement: Support passing custom headers to Elasticsearch destination connector**
* **Enhancement: Discord connector supports structured data outputs**

## [0.3.6]

* **Enhancement: Add airtable connector**

## [0.3.5]

* **Enhancement: Add biomed connector**
* **Enhancement: Add salesforce connector**
* **Enhancement: Add discord connector**
* **Enhancement: Add Elasticsearch destination connector**

## [0.3.4]

* **Enhancement: Improve file handling and processing**

## [0.3.3]

* **Enhancement: Support for SharePoint Online**
* **Enhancement: Add MongoDB destination connector**
* **Enhancement: Add DeltaTable destination connector**
* **Enhancement: Add S3 destination connector**
* **Enhancement: Additional file types to Confluence Connector**
* **Enhancement: Add Azure destination connector**

## [0.3.2]

* **Enhancement: Support for Local Filesystem connector**
* **Enhancement: Add connector for Confluence**
* **Enhancement: Support for Microsoft 365 Azure App for SharePoint Connector**
* **Enhancement: Support for output to multiple destinations**
* **Enhancement: Support for delta table destination format**
* **Enhancement: Optimize fetching files from SharePoint**

## [0.3.1]

* **Enhancement: Support for GCS (Google Cloud Storage) Connector**
* **Enhancement: S3 Connector**
* **Enhancement: SFTP Connector**
* **Enhancement: Support for output dir**
* **Enhancement: Add embedding support**
* **Enhancement: Support for Azure Blob Storage**
* **Enhancement: Support CSV source format**

## [0.3.0]

* **Enhancement: Support for SharePoint Connector**
* **Enhancement: Support for Elasticsearch Connector**
* **Enhancement: Support for various file handling capabilities, parallel processing, etc.**

## [0.2.2]

* **Enhancement: Add Outlook Connector**
* **Enhancement: Support for fsspec**
* **Enhancement: Support for json output format**

## [0.2.1]

* **Enhancement: Add Google Drive Connector**
* **Enhancement: Add HubSpot Connector**
* **Enhancement: Add Jira Connector**
* **Enhancement: Add Kafka Destination Connector**
* **Enhancement: Asynchronous processing for destinations**
* **Enhancement: Support for embedding providers including OpenAI, Huggingface, Instructor, Huggingface**

## [0.2.0]

* **Enhancement: Add Reddit Connector**
* **Enhancement: Support for GCS with s3 compatible mode**

## [0.1.1]

* **Enhancement: Support for Chroma output**
* **Enhancement: Support for Weaviate output**
* **Enhancement: Support for Pinecone output**
* **Enhancement: Support for OpenSearch output**

## [0.1.0]

* **Enhancement: Support for S3 file system**
* **Enhancement: Support for Azure file system**
* **Enhancement: Support for Google Cloud Storage file system**
* **Enhancement: Support for local file system**
* **Enhancement: Support for reprocessing and deduplication of files**
* **Enhancement: Support for multimodal embeddings for use with unstructured elements**
* **Enhancement: Support for multiple file types like PDFs, DOCX, HTML, TXT, email, etc.**

## [0.0.25]

* **Enhancement: Support for embedding via embedding providers**
* **Enhancement: Support for OpenSearch output**
* **Enhancement: Support for Elasticsearch v7+**

## [0.0.24]

* **Enhancement: Support for multiple file sources at once**
* **Enhancement: Support for Chroma database output**

## [0.0.23]

* **Enhancement: Support for Pinecone database output**

## [0.0.22]

* **Enhancement: Support for fsspec for S3 and Azure file systems**
* **Enhancement: Support for Weaviate database output**
* **Enhancement: Support for deduplication of files based on file hash**

## [0.0.21]

* **Enhancement: Support for multiple files for processing**
* **Enhancement: Support for recursive directory processing**

## [0.0.20]

* **Enhancement: Support for AWS S3**

## [0.0.19]

* **Enhancement: Support for Google Cloud Storage**

## [0.0.18]

* **Enhancement: Support for Azure Blob Storage**

## [0.0.17]

* **Enhancement: Support for local file system**

## [0.0.16]

* **Enhancement: Adding base functionality**

## [0.0.15]

* **Enhancement: Support for remote filesystems**
* **Enhancement: Support for chunking strategy**
* **Enhancement: Support for local filesystem**

## [0.0.14]

* **Enhancement: Support for additional input sources like Azure Blob Storage, Google Cloud Storage, and Amazon S3**
* **Enhancement: Support for additional output destinations like Elasticsearch and local filesystem**

## [0.0.13]

* **Enhancement: Command line interface (CLI) support**

## [0.0.12]

* **Enhancement: Support for Elasticsearch destination**

## [0.0.11]

* **Enhancement: Support for local file directory and filesystem**

## [0.0.10]

* **Enhancement: Support for multiple file sources and various file formats**

## [0.0.9]

* **Enhancement: Support for Azure Blob Storage file sources and destinations**
* **Enhancement: Support for Google Cloud Storage file sources and destinations**
* **Enhancement: Support for AWS S3 file sources and destinations**

## [0.0.8]

* **Enhancement: Support for various cloud storage file sources and destinations**

## [0.0.7]

* **Enhancement: Support for various file sources like local filesystem, cloud storage, and destinations**
* **Enhancement: Support for multithreaded processing and chunking strategies**

## [0.0.6]

* **Enhancement: Support for Elasticsearch destination**

## [0.0.5]

* **Enhancement: Support for various filesystem sources and destinations**
* **Enhancement: Support for fsspec for filesystem access**
* **Enhancement: Support for multiple content file types**

## [0.0.4]

* **Enhancement: Support for local filesystem and basic cloud storage destinations**
* **Enhancement: Support for multithreaded processing**

## [0.0.3]

* **Enhancement: Support for recursive directory processing**

## [0.0.2]

* **Enhancement: Command line interface (CLI)**

## [0.0.1]

* **Enhancement: Setting up the basic framework for ingest**
* **Enhancement: Support for local directories**
* **Enhancement: Support for individual file processing**
* **Enhancement: Support for various file formats (pdf, docx, pptx, xlsx, eml, html, xml, text)**

## [0.0.0]

* **Enhancement: Initial setup and project creation**
* **Enhancement: Basic ingestion framework and architecture**
