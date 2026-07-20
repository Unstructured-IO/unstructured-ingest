## [1.7.12]

### Fixes

- **Close Zendesk and Notion HTTP clients after connector operations.** Connector prechecks, indexing, downloads, context-manager entry failures and exits, and constructor-failure paths now release their underlying HTTP connection pools.

## [1.7.11]

### Fixes

- **fix(FS-2139): centralize bounded Slack API rate-limit retries in SDK client configuration.** Sync and async Slack clients now use the Slack SDK's connection and rate-limit retry handlers configured once in `SlackConnectionConfig`, replacing custom call-site retry loops across indexer join/history and downloader history/replies/files calls.

## [1.7.10]

### Fixes

- **fix(google-drive): record the drive id in each file's record locator.** The indexer used an annotation statement (`record_locator["drive_id"]: object_id`) instead of an assignment, so `drive_id` was silently never written into `record_locator`. Changed to a real assignment so downstream consumers can resolve which drive a file came from.

## [1.7.9]

### Fixes

- **test(teradata): extend the log-redaction regression suite to all seven logger.error sites.** Adds secret-leak tests for the `create_destination`, `delete_by_record_id`, and insert-batch (`executemany`) driver-error branches, which previously had no coverage — each plants a credential in the driver exception and asserts it never reaches the logs.

## [1.7.8]

### Fixes

- **fix(security): redact provider exceptions from Teradata error logs and user-fault responses.** The seven `logger.error(..., exc_info=True)` sites route through `safe_error_summary` with `exc_info` dropped; `_raise_classified_teradata_error` no longer interpolates the raw driver message into the user-fault `UserError` (`Teradata reported: {exc}`) — it surfaces only the numeric code + fixed descriptor — and both classified raises use `from None`, so the driver text (host/user/password) can't reach the response or resurface via traceback logging.

## [1.7.7]

### Fixes

- **fix(milvus): align nonexistent-db precheck test with the wrapped error message.** `test_precheck_fails_on_nonexisting_db` now asserts on the `"failed to precheck Milvus"` message raised by the uploader's precheck wrapper instead of the raw upstream `"database not found"` text, so the test no longer breaks when the underlying Milvus client error string changes.

## [1.7.6]

### Fixes

- **fix(security): redact raw exceptions from Salesforce/Chroma non-precheck raises.** Salesforce private-key parse failures and Chroma upsert failures route the caught exception through `safe_error_summary` and re-raise `from None`, so key material / driver text no longer surfaces in the raised error or its chained traceback.

## [1.7.5]

### Fixes

- **fix(security): redact provider text from fsspec/watsonx wrap_error classified branches.** Dropbox/Box/Azure/GCS/watsonx `wrap_error` route classified-branch messages through `safe_error_summary` instead of raw provider text (`e.body`/`e.message`/`e.reason`/full request URL); watsonx strips the URL query string from logs and now classifies HTTP 500 as `ProviderError` (was `> 500`).

## [1.7.4]

### Enhancements

- **feat(PLU-441): ready the GitHub source connector for Foundation.** Adds OAuth support (`oauth_token`/`refresh_token` alongside the existing PAT, refresh handled upstream per PLU-381) and emits the git blob SHA as `metadata.version` for incremental change detection. Indexing fetches the repo once per run instead of twice per file, falls back to a per-directory walk when GitHub truncates the recursive tree, and skips empty files, directories, and submodules. Fixes downloads failing with `FileNotFoundError` on nested paths. GitHub API errors are classified consistently (auth → `UserAuthError`, throttle → `RateLimitError`, 5xx → `ProviderError`), and transient throttles / 5xx / network errors are retried with exponential backoff that honors GitHub's `Retry-After` (`max_retries`, default 10).

## [1.7.3]

### Fixes

- **fix(security): redact provider exceptions from missed connector error sinks.** VastDB/DuckDB `precheck` overrides and the Databricks/Zendesk/S3 unhandled-exception logs no longer interpolate raw exceptions or dump `exc_info` tracebacks; the Zendesk/S3/Databricks `wrap_error` classified branches route returned messages through `safe_error_summary`; Zendesk now classifies HTTP 500 as `ProviderError`.

## [1.7.2]

### Fixes

- **fix(security): redact MotherDuck token from precheck connection errors.** `MotherDuckUploader.precheck` now routes caught exceptions through `safe_error_summary` and drops `exc_info`, so a bad-token `OperationalError` no longer leaks `md:?motherduck_token=<token>` into logs or the API response.

## [1.7.1]

### Fixes

- **docs(security): document that `safe_error_summary`'s allowlist rests on attribute names.** Adds a comment in `error.py` recording that `_MACHINE_CODE_RE` is a shape filter, not a secret detector, so the guarantee depends on keeping `_SAFE_ERROR_ATTRS` limited to fields that never carry free text. Comment-only; no runtime change.

## [1.7.0]

### Enhancements

- **feat(sftp): verify SFTP server host key.** `SftpConnectionConfig` gains an optional `host_public_key` field. It accepts a base64 blob, an OpenSSH `.pub` line, or an ssh-keyscan/known_hosts line; the key algorithm (`ssh-ed25519` / `ssh-rsa`) is auto-detected from the key itself, so no key-type input is required. When set, the connector pins the expected host key and uses paramiko `RejectPolicy` to verify the server identity, rejecting a mismatched/unknown key instead of fsspec's default `AutoAddPolicy` (which silently trusts any key). An unparsable or unsupported key fails fast at config validation; a well-formed but wrong key surfaces as a host-key mismatch on the first connect (precheck). When `host_public_key` is omitted, connections behave as before but now log a warning that the server identity is unverified. Username/password remains the client authentication method in both cases.

## [1.6.32]

### Enhancements

- **feat: Slack indexer emits records newest-first within each channel.** `SlackIndexer.run` sorts each channel's records (conversation-day packages and file attachments) by `metadata.version` descending before yielding, so the most recent content is indexed first.

## [1.6.31]

### Fixes

- **fix(security): redact credential-bearing exception details from connector error logging.** Connector precheck, upload, and path-validation error paths — and the central `UnstructuredIngestError.wrap` decorator — interpolated caught exception text into logged and raised messages, which could echo credentials (tokens, passwords, DSNs, service-account JSON). These messages now carry a sanitized summary via the new `safe_error_summary()` helper: the exception type name plus allowlisted machine-readable fields (integer HTTP statuses, SDK error-code enums, opaque request/correlation IDs — e.g. `SlackApiError(status_code=401, error=invalid_auth)`), never free-form exception text. `exc_info` frame dumps were removed from credential-bearing paths (a rendered traceback's terminal line re-embeds `str(e)`). The GCS connector also no longer echoes raw service-account JSON via `OSError: File name too long` when key material is passed where a filename is expected. Exception classes and control flow are unchanged; only log and error-message payloads were sanitized.
- **fix(security): preserve connector-authored guidance and harden the redaction.** Connector prechecks re-raise dependency-install hints (`requires_dependencies`) and connector-authored typed errors (missing index/database/collection, SCRAM-SHA-1 remediation, OAuth error codes, corpus-name mismatch) instead of flattening them to a bare type name. Every redacted re-raise now suppresses the provider exception chain (`from None`) so its text cannot resurface through traceback logging. The GCS service-account guard is narrowed to the too-long-filename case (`ENAMETOOLONG`) so a genuine filesystem error on a real path is no longer masked as "Invalid auth token value". Elasticsearch bulk-upload failure logs now allowlist safe fields (`_index`, `_id`, `status`, `error.type`) instead of only stripping the uploaded document, so document content in `error.reason` is no longer logged.

## [1.6.30]

### Fixes

- **fix(ENG-1322): escape SQL metacharacters in databricks_volume_delta_tables statements.** Filenames or record identifiers containing a single quote or backslash no longer break (or inject into) the single-quoted `PUT`/`DELETE` string literals, and filenames containing a backtick no longer break the backtick-quoted volume path in the `INSERT ... FROM json.` clause (all surfaced as SQLSTATE 42601). A shared `quote_literal` helper escapes both backslashes and single quotes (Databricks processes backslash escapes in string literals by default) for the volume/staging paths and the delete record identifier, and the `INSERT` source path now goes through the existing `quote_identifier` helper.

## [1.6.29]

### Fixes

- **fix(sharepoint): surface the real upstream HTTP status instead of masking every error as "Site not found".** SharePoint upstream errors now map to typed exceptions carrying the real status code and response body (instead of a generic `400`), and genuine throttles are retried, honoring the server's `Retry-After` backoff.

## [1.6.28]

### Fixes

- **fix(FS-2108): download Jira attachment content to the correct path with attachment-specific display names.** Attachment downloads now write bytes to the attachment's own `source_identifiers` path instead of a separate `attachments/` directory, and each attachment gets its filename as `display_name` instead of inheriting the parent issue title. Attachment download paths are validated to stay within `download_dir` so crafted filenames cannot escape via path traversal.

## [1.6.27]

### Fixes

- **fix(FS-2106): populate Jira creation and modification dates at index time.** `JiraIndexer._create_file_data_from_issue` only set `version` (from the issue's `updated` timestamp) and never populated `date_created`/`date_modified`, and the indexer field lists omitted `created`. Because the platform detects new and modified records from the indexer's `FileData.metadata`, Jira records carried no creation/modification dates. The indexer now requests the `created` field in `_get_issues_within_projects`, `_get_issues_within_single_board`, and `_get_issues_by_keys`, and sets `metadata.date_created` (from `created`) and `metadata.date_modified` (from `updated`) as Unix epoch strings alongside the existing `version`.

## [1.6.26]

### Fixes

- **fix(FS-2105): populate Confluence creation/modification dates and version at index time.** The Confluence indexer now sets `date_created`, `date_modified`, and `version` from the v2 pages list response so Foundation can store page timestamps and detect page edits on subsequent runs (fixes FS-2107).

## [1.6.25]

### Enhancements

- **feat(weaviate): add `auto_schema` option to the destination connector.** New uploader option that defaults to `false`. When `false`, the connector behaves exactly as before: the collection must already exist (or, in non-flatten mode, is seeded from the default config), and in `flatten_metadata` mode each object is conformed to the existing schema (unknown properties dropped, missing ones set to null). When `true`, the connector skips the schema fetch/conform step and lets Weaviate create the collection and its columns from the uploaded objects on first insert, so a collection does not need to exist up front (requires `AUTOSCHEMA_ENABLED=true` in Weaviate). Combined with `flatten_metadata=true`, this allows dynamic, up-front-unknown metadata to land as top-level columns without a predefined schema.

## [1.6.24]

### Fixes

- **fix(slack): group channel messages into stable per-UTC-day packages for incremental sync.** The Slack indexer now emits one conversation package per channel per UTC day with a stable identifier derived from channel and day. `metadata.version` tracks the newest activity in the package (new messages, thread replies via `latest_reply`, edits via `edited.ts`) so re-runs update in place instead of duplicating documents.

## [1.6.23]

### Fixes

- **fix(teradata): auto-create the destination table during upload, not only during pipeline init.** Table auto-creation ran only via `TeradataUploader.init()`, whose sole caller is the local `Pipeline`. Orchestrators that drive only the upload phase (e.g. the Unstructured Platform) never created the table, so a not-pre-created destination failed with Teradata error 3807 (`Object '<table>' does not exist`). `upload_dataframe` now ensures the table exists itself — once per run, via the idempotent `create_destination()` whose `DBC.TablesV` check skips `CREATE` for existing tables, so a user's table is never recreated.

## [1.6.22]

### Enhancements

- **feat(slack): user token specific behavior** - differentiate between user and bot token in slack indexer, bot still attempts to join channels (required for ingestion) while user reads channel without joining. Add token specific error messages.

## [1.6.21]

### Fixes

- **fix(embed): fix Azure OpenAI embedding precheck failing on valid deployments.** The precheck rejected Azure deployments whose name differed from the base model (the normal Azure setup), because it checked against the base-model catalog rather than the deployment. It now validates the deployment with a real test embedding call, so correctly configured deployments pass and a missing deployment reports a clear error.

## [1.6.20]

### Fixes

- **test(stager): de-flake the stager bounded-memory tests.** `test_process_whole_peak_memory_is_flat_as_input_grows` and `test_blob_store_stager_peak_memory_is_flat_as_input_grows` asserted a near-flat `tracemalloc` peak ratio between two input sizes. `tracemalloc`'s peak includes not-yet-collected per-element garbage, so the ratio varies with GC timing across environments and the blob-store test failed on CI even though the streamed peak (~3.8 MB) was a small fraction of the ~47 MB input. The tests now assert the meaningful streaming-vs-whole-file bound — peak stays below the input file size (a whole-file load materializes the parsed list at several times the JSON text) — which is robust to GC timing while still catching a regression to whole-file loading.

## [1.6.19]

### Fixes

- **fix(stager): stream the `.json` element file instead of loading it whole, to stop OOM on large documents.** `UploadStager.process_whole` (the path taken when the partition output has a `.json` suffix) previously called `json.load` on the entire element file, built a full conformed `list`, and wrote it whole — holding 3+ full copies of a several-hundred-MB file resident. A very large document (tens of thousands of pages) OOM-killed the stager plugin (SIGKILL, zero app logs — the classic OOMKill signature) at this step. `process_whole` now iterates the JSON array element-by-element via `ijson` (new `data_prep.json_stream`), conforms each element, and stream-writes the output (new `data_prep.write_data_streaming`), keeping only one element resident at a time — matching the bounded-memory profile of the existing `.ndjson` `stream_update` path. Output is byte-for-byte identical to the previous `write_data` result. The streamed write is atomic (temp file + `os.replace`) so it stays correct when the stager is called with the output path equal to the input path — a plain in-place `open(path, "w")` would truncate the file before the lazy reader finished, and a mid-stream failure leaves any existing artifact untouched. `ijson` is stricter than stdlib `json`, so the rare `NaN`/`Infinity`/`>int64` inputs that `json.load` accepts fall back to a buffered read. `BlobStoreUploadStager` (used by blob-store destinations such as S3) overrides `run` with the same `get_json_data` + `write_data` whole-file load and so bypassed `process_whole` entirely — it now delegates to the streamed `process_whole` (it is a pure format copy, so `conform_dict` is identity), closing the actual OOM path observed on the blob-store stager. Adds `ijson` as a base dependency. The base uploader (`Uploader.run`) has the same whole-file-load problem but its fix requires a shared-interface change and is tracked separately.

## [1.6.18]

### Fixes

- **test(ci): stabilize and de-gate flaky live-service integration tests.** Two live-service checks reddened unrelated PRs repo-wide. (1) The SharePoint source tests byte-diffed `metadata.permissions_data` — a live snapshot of the site's ACLs (user/group object ids) that drifts whenever tenant sharing changes — against checked-in fixtures, producing a consistent "Diffs found" failure with no connector regression. `permissions_data` is now excluded from the comparison, consistent with the existing exclusions of `date_*`, `LastModified`, and the Graph download/url fields; permissions extraction remains covered by unit tests. (2) The hosted-API partitioner test intermittently hung/timed out or dropped its connection; the live call is now wrapped in a bounded retry (`retry_async`) that retries only on transient failures (5xx, timeout, transport read/connect errors) with a per-attempt timeout, while non-transient errors still fail immediately. Finally, live-service e2e jobs (which require external credentials and are inherently flaky) now run nightly, on demand, and post-merge instead of blocking every PR; the creds-free `check_untagged_tests` guard still runs on PRs.

## [1.6.17]

### Enhancements

- **feat(slack): auto-join public channels and include url in metadata.** Have slack connector attempt to join public channels automatically and verbosly report in an error when channel cannot be accessed for any reason. Include a permalink to the file or channel message in `FileData`

## [1.6.16]

### Fixes

- **fix(weaviate): declare `metadata.data_source.version` explicitly as `text` to prevent auto-schema uuid inference.** When the destination collection did not yet exist, Weaviate's auto-schema typed `metadata.data_source.version` based on the value shape of the first inserted record; UUID-formatted source ETags caused the property to be locked as `uuid`, after which later records carrying non-UUID-shaped identifiers (e.g. multipart `<hex>-<part_count>` ETags, or opaque version strings from source connectors that do not return UUID-formatted ETags) were rejected with `WeaviateInsertManyAllFailedError: requires a string of UUID format`. The collection config now declares the field as `text` at creation time so auto-schema does not fire for it. The existing `str()` cast in `WeaviateUploadStager.conform_dict` is harmless and stays. Other `metadata.*` fields remain auto-schemed; this is the minimum change to unblock the observed failure mode without expanding the static schema.

## [1.6.15]

### Fixes

- **fix(sharepoint): return all principals in document `permissions_data` instead of a single collapsed identity.** `office365-rest-python-client` shares one mutable-default `Identity` singleton across every `Permission` deserialization, so SDK typed reads collapsed every document's permissions to whichever user/group was deserialized last. The connector now bypasses the SDK's typed accessors and parses raw Graph `/$batch` JSON directly, so per-document users and groups round-trip distinctly. Reported by Intel.

### Enhancements

- **feat(onedrive): expose document ACLs in `permissions_data`.** OneDrive previously always returned `permissions_data: null`. The indexer now performs the same chunked Graph `/$batch` permission fetch as SharePoint and writes the canonical `read` / `update` / `delete` buckets (matching the Google Drive / Confluence schema) into `metadata.permissions_data`. Empty fetches and per-item failures degrade to the previous behavior. `tenacity` is added to the `onedrive` and `sharepoint` extras.

## [1.6.14]

### Fixes

- **fix(connectors): stop asserting environment-specific URLs in Notion/OneDrive/SharePoint source integration tests.** The expected-results diff compared `additional_metadata.url` (Notion page id, MS Graph drive id) and `@microsoft.graph.downloadUrlNoAuth` (a tokenized no-auth download link), both of which vary by test tenant/workspace and per request. With no connector code change, these drifted and reddened `blob_storage_connectors_int_test` and `uncategorized_connectors_int_test` on main and every PR. Both fields are now excluded from the comparison, consistent with the existing exclusions of `@microsoft.graph.downloadUrl`, `LastModified`, and `date_*`.

## [1.6.13]

### Fixes

- **fix(weaviate): set `vectorIndexType: "hnsw"` in the auto-created collection schema.** The default Weaviate collection config (`unstructured_ingest/processes/connectors/assets/weaviate_collection_config.json`) declared `vectorizer: "none"` but left `vectorIndexType` unset. Newer Weaviate server versions no longer infer a vector index when none is specified, so collections created via `WeaviateUploader.create_destination` came up without an index — vectors uploaded by the pipeline could not be queried with `near_vector` / `hybrid`, returning empty results. The schema now declares `hnsw` explicitly so auto-created collections are immediately searchable. Existing user-managed collections (`flatten_metadata=true`) are unaffected.

## [1.6.12]

### Enhancements

- **feat(atlassian): support OAuth-backed Jira and Confluence cloud sources.** Jira and Confluence source configs accept Atlassian OAuth access tokens for API gateway requests with cloud IDs, while refresh tokens are carried for platform-side rotation before job dispatch. Confluence v2 indexing now paginates space and page discovery up to the configured limits so large tenants are not truncated.

## [1.6.11]

### Fixes

- **fix(google-drive): exclude trashed items from indexer queries.** The Google Drive connector's `files.list` calls filtered by parent id only, so items the user had moved to Drive's trash were pulled and ingested like live content — "delete in Drive UI" did not remove the file from the corpus on the next ingest. Each list-query call site (`count_files_recursively`, the non-recursive precheck empty-folder probe, and `get_paginated_results`) now appends `and trashed = false`, matching the Drive v3 query semantics that exclude trash explicitly.

## [1.6.10]

### Fixes

- **fix(databricks-delta-tables): stage each source file at its full relative path.** The `databricks_volume_delta_tables` uploader keyed the staging volume path on the source filename alone, so distinct source files sharing a basename across different folders overwrote each other on the volume — leading to dropped or duplicated rows and intermittent load failures under concurrency. It now uses the full relative path (falling back to the filename), matching the Databricks Volumes and object-store destinations. (PLU-392)

## [1.6.9]

### Enhancements

- **feat(weaviate): add `flatten_metadata` option to the Weaviate uploader.** Opt-in, default off. When set, the stager flattens element `metadata` into top-level properties (lists pass through unmodified) and skips all default-mode coercions (date reformatting, JSON-stringification of `record_locator` / `coordinates.points` / `links` / `permissions_data` / `regex_metadata`, string casting of `version` / `page_number`). The uploader requires an explicit pre-created collection — `create_destination` no-ops, and `precheck` validates collection existence and the presence of `record_id_key` so re-run delete-by-record-id continues to work. Per-element, properties not declared on the user's schema are dropped and missing schema properties are filled with `None`. Default behavior (`flatten_metadata=False`) is unchanged.

## [1.6.8]

### BREAKING

- **`UserError.status_code` changed from `401` to `422`.** `401` is HTTP "Unauthorized" (unauthenticated) — that's what `UserAuthError` covers. `UserError` is for invalid user input / config, which is HTTP `422` (Unprocessable Entity). Subclasses that override `status_code` (`UserAuthError=401`, `RateLimitError=429`) are unaffected. `QuotaError` inherits without override and now reports `422` — callers matching on `status_code == 401` for quota errors must update.

### Enhancements

- **feat(stager): add `should_include` filter hook to `UploadStager` base class.** New predicate defaults to `True`, preserving behavior across every existing connector. Subclasses override `should_include(element_dict)` to drop elements their destination cannot accept, replacing the prior pattern of duplicating `stream_update` and `process_whole` just to insert a one-line filter.

### Fixes

- **fix(teradata): classify driver errors and surface user-fault TD messages.** Adds `_raise_classified_teradata_error` that inspects `[Error NNNN]` codes in `teradatasql` exceptions and re-raises recognised user-fault codes (`3807` object-missing-or-no-privilege, `3523` / `5612` / `5315` no privilege, `3706` / `3707` SQL syntax, `3753` / `3754` implicit type conversion) as `UserError` with the original Teradata message preserved via `"Teradata reported: …"`. Wraps `cursor.execute` in `TeradataUploader.get_table_columns`, `delete_by_record_id`, `upload_dataframe`, `create_destination`, and the `TeradataIndexer.precheck` table probe. Non-`teradatasql` exceptions (e.g. `MemoryError`, `OSError`) re-raise unchanged; unrecognised TD codes fall through to the existing `DestinationConnectionError` / `SourceConnectionError` wrapping so retry behaviour is preserved. (PLU-377)
  - **Source-side behavior change:** The classification applies to both directions. `TeradataIndexer.precheck` previously always raised `SourceConnectionError` when its table probe failed; it now raises `UserError` for codes in the recognised map (3807, 3523, 5612, 5315, etc.) and `SourceConnectionError` only for unrecognised codes / network failures. Source-side callers that catch `SourceConnectionError` will no longer catch those cases.
- **fix(milvus): drop elements without embeddings before insert.** Empty-text elements (e.g., page-boundary `UncategorizedText` produced by the partitioner) are skipped by the embedder and arrive without an `embeddings` key. Milvus rejected these inserts with `Insert missed an field 'embeddings' to collection without set nullable==true or set default_value`, failing the entire workflow. `MilvusUploadStager` now overrides `should_include` to filter these elements out before they reach the uploader.

## [1.6.7]

### Enhancements

- **feat(box): OAuth 2.0 access token support on the Box source.** `BoxAccessConfig` now accepts `access_token` (with optional `refresh_token`) alongside the existing `box_app_config` field, and a validator enforces exactly one auth method. When `access_token` is set, `BoxConnectionConfig.get_access_config()` returns a `boxsdk.OAuth2` client. `refresh_token` is stored as a carrier for external refresh flows (e.g. orchestrator-side rotation) and is not consumed in-process.

## [1.6.6]

### Enhancements

- **feat(embed): add `CustomOpenAICompatibleEmbeddingConfig` subclass.** New optional config for OpenAI-compatible gateways (vLLM, NIM, Ollama, LiteLLM, etc.) that authenticate via custom HTTP headers. Adds optional `api_key: SecretStr | None` and `default_headers: dict[str, SecretStr] | None` on the subclass. When `api_key` is unset, no `Authorization` header is emitted to the gateway. Existing `OpenAIEmbeddingConfig` / `AzureOpenAIEmbeddingConfig` surfaces are unchanged.

### Fixes

- **test(notion): make `test_notion_source_database` row-order insensitive.** Test-only change; no published behavior.

## [1.6.5]

### Fixes

- **fix(google-drive): skip non-downloadable native files during indexing and download.** Google Drive shortcuts, forms, maps, sites, fusiontables, and jams are now silently filtered out during indexing rather than failing at download time. Empty placeholder files (`inode/x-empty` or zero-byte with no MIME type) are also skipped. The downloader returns `None` for these files instead of raising `SourceConnectionError`, and `count_files_recursively` excludes them from file counts.

## [1.6.4]

### Enhancements

- **feat(slack): support file attachments and OAuth refresh tokens.** Slack indexing now emits file attachment records, downloading uses Slack private file URLs with bearer authentication, and `SlackAccessConfig` accepts `refresh_token` so platform plugin schemas can expose OAuth token rotation settings.

### Fixes

- **fix(slack): guard private file downloads.** Validate Slack private download URLs before sending bearer credentials, refuse redirects that could forward bearer credentials, stream private file downloads to disk, and use a bounded timeout for private file reads.
## [1.6.3]

### Enhancements

- **feat(databricks): add `flatten_metadata` option to the Volumes Delta Tables uploader.** Opt-in, default off. When set, the stager flattens element metadata into top-level columns matching Milvus's unprefixed naming, and the uploader skips auto-create against the user-managed table, dropping unknown incoming columns with a log line.

## [1.6.2]

### Fixes

- **fix(google-drive): export Google-native Docs Editors files before download.** Google Docs, Sheets, Slides, and Drawings now use Drive export endpoints instead of raw media downloads, preventing 403 errors for files without binary content. Extension filters now include matching Google-native MIME types for exportable formats, and unsupported Google-native files fail clearly instead of falling through to direct download.

## [1.6.1]

### Enhancements

- **feat(oauth): add `refresh_token` field to Google Drive, GCS, OneDrive, SharePoint, and Outlook AccessConfigs.** Allows the platform to persist and retrieve OAuth refresh tokens alongside access tokens, enabling automatic token refresh before job dispatch. Dropbox already had this field.

## [1.6.0]

### Enhancements

- **feat(box): pass through ACL permission metadata.** Extract Box collaboration data and normalize to the standard read/update/delete schema. Permissions are fetched during indexing with an LRU-cached ancestor folder walk to handle inherited collaborations, plus a per-parent-folder `path_collection` cache so only the first file in a given parent pays the `file.get()` round-trip. Access-only collabs (`is_access_only=true`) are skipped to avoid overgranting; group IDs are stored directly without member expansion (consistent with Confluence). `boxsdk` is now installed via the `box` extra. Both the permissions cap and ancestor-cache size are configurable on `BoxIndexerConfig` (`max_num_metadata_permissions`, `permissions_cache_max_size`) and `BoxDownloaderConfig` for the standalone fallback path.

### Fixes

- **fix(test): strip randomized tempdir prefix from FsspecDownloader fixture paths.** `get_files()` now drops the leading `unstructured_<random>/` segment so `directory_structure.json` captures the logical structure rather than the per-run random suffix injected by `tempfile.mkdtemp`.

## [1.5.2]

### Enhancements

- **feat(microsoft): add delegated `oauth_token` to SharePoint, OneDrive, and Outlook AccessConfigs.** Accepts a user access token directly, bypassing MSAL when present. `client_id` / `client_cred` become optional. Mirrors the Google Drive `oauth_token` pattern; refresh is not handled here.

## [1.5.1]

### Fixes

- **fix(azure_ai_search): recursively drop unknown fields against the index schema.** New nested fields from `unstructured` (e.g. `metadata.table_extraction_method`) were reaching the service and causing HTTP 400s; the filter now recurses through `index.fields`.

## [1.5.0]

### Enhancements

- **feat(sharepoint): pass through ACL permission metadata.** Extract SharePoint permission data from the Graph API and normalize to the standard read/update/delete schema. Permissions are fetched via Graph JSON batching with per-item fallback on batch failure.

## [1.4.29]

### Chores

- **chore(tests): temporarily skip Confluence integration test pending account reactivation.**

## [1.4.28]

### Fixes

- **fix(databricks): backtick-quote catalog and database in `USE CATALOG` / `USE DATABASE`** Fixes `SQLSTATE 42602 / INVALID_IDENTIFIER` for names with hyphens or other non-word characters.

## [1.4.27]

### Fixes

- **fix(teradata): fix off-by-one in split_dataframe that sent empty executemany batches** The old `num_chunks = len(df) // chunk_size + 1` formula always appended one extra iteration. When a document produced exactly N * batch_size elements, the last chunk was empty, causing teradatasql to send a zero-row parameterized batch to the server which returned Error 3939 (parameter count mismatch). Replace with `range(0, len(df), chunk_size)` which never produces an empty slice. All SQL-based connectors (Teradata, Snowflake, VastDB, Databricks Delta Tables, KDB.AI) share this function and benefit from the fix.

## [1.4.25]

### Security

- **security:** fix(deps): upgrade vulnerable transitive dependencies [security]

## [1.4.24]

### Enhancements

- **refactor(teradata): use format_destination_name pattern for table name sanitization** Replace the `sanitize_destination_name: ClassVar[bool]` flag on the base `SQLUploader` with an overridable `format_destination_name` method on `TeradataUploader`, matching the convention used by AstraDB, Pinecone, and Weaviate connectors. Sanitization logic is now local to the Teradata connector.

## [1.4.23]

### Fixes

- **fix(fixtures): update SharePoint and Notion integration test fixtures** SharePoint API now returns `isAuthoritative` in `additional_metadata`; Notion page/database HTML content updated to reflect current upstream API output.
- **fix(teradata): sanitize destination table names** Replace invalid characters (including dashes) with underscores in Teradata table names during `create_destination`, preventing cryptic database errors when embedding model names or workflow IDs are used as table name components.

## [1.4.22]

### Fixes

- **fix(teradata): honour HTTP_PROXY/HTTPS_PROXY for proxy-restricted environments** The teradatasql Go driver does not automatically read standard proxy environment variables. Added `_build_proxy_params()` which reads `HTTPS_PROXY`, `HTTP_PROXY`, and `NO_PROXY` at connection time and forwards them as native teradatasql parameters (`https_proxy`, `http_proxy`, `proxy_bypass_hosts`), enabling Teradata connections in environments that require an egress proxy.

## [1.4.21]

### Fixes

- **fix(azure): make anonymous access explicit for public containers** Set `anon=True` when Azure source config omits credentials and `anon=False` when explicit credentials are provided, so public-container access no longer depends on the `adlfs` default that changed in `2026.4.0`.

## [1.4.20]

### Fixes

- **fix(teradata):** add embeddings column to destination schema and stager, use native VECTOR32 type (32-bit float) for embeddings column
* **fix(teradata): reject dashes in destination table names** Add validation to prevent dashes in Teradata table names at both the Pydantic model level and the `create_destination` path, surfacing a clear error message instead of a cryptic database failure.

## [1.4.19]

### Security

- **fix(deps):** loosen langchain-core pin from `<1.0.0` to `<2.0.0` to resolve CVE-2026-34070 (High) and CVE-2026-26013 (Low)

## [1.4.18]

### Security

- **security:** fix(deps): upgrade vulnerable transitive dependencies [security]

## [1.4.17]

* **fix(teradata): surface user-friendly error messages on connector precheck failures** Instead of exposing raw Go driver stack traces, precheck errors now show concise messages (e.g. "Failed to connect to server 192.168.1.1: connection timed out").

## [1.4.16]

* **fix(notion): handle icon field in block type deserialization** Skip the `icon` field when deserializing Notion block types (heading, paragraph, numbered_list, quote, table_of_contents, template, todo, toggle) to prevent deserialization errors when blocks contain icon data.

## [1.4.15]

* **fix(ibm-watsonx-s3): fail fast on precheck and guard bearer token JSON parsing**

## [1.4.14]

* **fix(teradata): enable Unicode Pass Through on session to prevent Error 6705 on non-BMP characters**

## [1.4.13]

* **fix(snowflake): handle VARIANT columns with PARSE_JSON during upload** Detect VARIANT-typed columns via `SHOW COLUMNS` and apply `PARSE_JSON()` in the `INSERT ... SELECT` statement; serialize `dict`/`list` values to JSON strings in `prepare_data` so structured metadata fields are stored correctly in VARIANT columns.

## [1.4.12]

* **fix(azure-ai-search): drop fields not present in index before upload** Filter document fields against the index schema before uploading to prevent errors when elements contain extra fields that the index doesn't define.

## [1.4.11]

* **fix: Parse `dict` values to JSON strings in SQL stagers**

## [1.4.10]

* **fix: Teradata opinionated schema CLOB column now uses CHARACTER SET UNICODE** 

## [1.4.9]

* **feat: Teradata auto-creates missing tables with opinionated JSON schema** When a provided table name does not exist, `create_destination()` now creates it automatically instead of failing at precheck.

## [1.4.8]

* **feat: Teradata opinionated writes with JSON metadata schema** Add auto-table-creation for Teradata destination when `table_name` is not provided. Uses a new 6-column schema (`id`, `record_id`, `element_id`, `text`, `type`, `metadata` as JSON) instead of the flattened 22-column layout. Controlled by `metadata_as_json` flag on the stager config (default `False` for backward compatibility). Follows the same `create_destination()` pattern used by Databricks Volume Delta Tables, Pinecone, AstraDB, and Weaviate.

## [1.4.7]

* **fix: SQL downloader KeyError when `fields` config omits `id_column`** Ensure `id_column` is always included in the SELECT field list across all SQL connectors (Teradata, PostgreSQL, Snowflake, SingleStore, SQLite).
* **fix: case-insensitive column resolution in SQL download responses** Add `_resolve_column_name` to the base `SQLDownloader` so `generate_download_response` handles databases that return uppercase column names (e.g., Snowflake, Teradata Enterprise).
* **fix: case-insensitive `can_delete()` in SQL uploader** The base `SQLUploader.can_delete()` now matches `record_id_key` case-insensitively against database column names, fixing silent delete skips on Snowflake and other case-folding databases.

## [1.4.6]

* **fix: SQL connector connection leak when commit() fails** Wrap `commit()`/`close()` in `try/finally` across Teradata, PostgreSQL, Snowflake, SingleStore, and SQLite so `close()` is always called.
* **fix: Teradata case-insensitive column handling** Resolve column names to actual database case before use in double-quoted SQL identifiers. Handles all uppercase/lowercase combinations between database and user config.
* **fix: Teradata uploader precheck validates table existence** Catch wrong table names early at precheck time instead of failing mid-pipeline.

## [1.4.5]

* **fix: add capability to use opensearch serverless**

## [1.4.4]

* **fix: add table precheck to teradata source**

## [1.4.3]

* **fix: enable sftp to make directories**

## [1.4.2]

* **chore: enable PyPI trusted publishing (OIDC)** Switch `pypa/gh-action-pypi-publish` from API token authentication to OIDC trusted publishing. Pin all GitHub Actions to commit SHAs, add concurrency control, a release-tag version validation step, and conditional Azure Artifacts upload.

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
