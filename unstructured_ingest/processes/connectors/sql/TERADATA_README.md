# Teradata SQL Connector

A production-ready connector for reading from and writing to Teradata databases using the Unstructured Ingest platform.

## 🎯 Overview

This connector provides both **source** (read) and **destination** (write) capabilities for Teradata databases, following the DBAPI 2.0 standard using the `teradatasql` driver.

**Key Features:**
- ✅ Full source and destination support
- ✅ Batch processing for efficient data transfer
- ✅ Automatic data type handling and conversion
- ✅ Upsert behavior (delete-then-insert)
- ✅ Flexible connection options (database, port)
- ✅ Production-tested and battle-hardened

**Implementation Status:** Phase 2 Complete (Flexible parameters)

---

## 📦 Installation

```bash
pip install "unstructured-ingest[teradata]"
```

This installs the base package plus the `teradatasql` driver.

---

## 🚀 Quick Start

### Destination (Write to Teradata)

```python
from pathlib import Path
from pydantic import Secret
from unstructured_ingest.interfaces import ProcessorConfig
from unstructured_ingest.pipeline.pipeline import Pipeline
from unstructured_ingest.processes.connectors.local import (
    LocalIndexerConfig,
    LocalDownloaderConfig,
)
from unstructured_ingest.processes.connectors.sql.teradata import (
    TeradataAccessConfig,
    TeradataConnectionConfig,
    TeradataUploaderConfig,
)
from unstructured_ingest.processes.partitioner import PartitionerConfig

Pipeline.from_configs(
    context=ProcessorConfig(
        work_dir="./workdir",
        verbose=True,
    ),
    indexer_config=LocalIndexerConfig(input_path="./documents"),
    downloader_config=LocalDownloaderConfig(),
    partitioner_config=PartitionerConfig(strategy="fast"),
    uploader_config=TeradataUploaderConfig(
        table_name="elements",
        batch_size=50,
    ),
    destination_connection_config=TeradataConnectionConfig(
        access_config=Secret(
            TeradataAccessConfig(password="your_password")
        ),
        host="your-teradata-host.com",
        user="your_user",
        database="your_database",  # Optional
        dbs_port=1025,              # Optional (default: 1025)
    ),
).run()
```

### Source (Read from Teradata)

```python
from pathlib import Path
from pydantic import Secret
from unstructured_ingest.interfaces import ProcessorConfig
from unstructured_ingest.pipeline.pipeline import Pipeline
from unstructured_ingest.processes.connectors.local import LocalUploaderConfig
from unstructured_ingest.processes.connectors.sql.teradata import (
    TeradataAccessConfig,
    TeradataConnectionConfig,
    TeradataIndexerConfig,
    TeradataDownloaderConfig,
)
from unstructured_ingest.processes.partitioner import PartitionerConfig

Pipeline.from_configs(
    context=ProcessorConfig(
        work_dir="./workdir",
        verbose=True,
    ),
    indexer_config=TeradataIndexerConfig(
        table_name="source_table",
        id_column="doc_id",
        batch_size=100,
    ),
    downloader_config=TeradataDownloaderConfig(
        download_dir="./downloads",
        fields=["doc_id", "content", "title"],  # Optional: specific columns
    ),
    source_connection_config=TeradataConnectionConfig(
        access_config=Secret(
            TeradataAccessConfig(password="your_password")
        ),
        host="your-teradata-host.com",
        user="your_user",
        database="your_database",  # Optional
    ),
    partitioner_config=PartitionerConfig(strategy="fast"),
    uploader_config=LocalUploaderConfig(output_dir="./output"),
).run()
```

---

## ⚙️ Configuration Options

### Connection Configuration

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `host` | str | ✅ Yes | - | Teradata server hostname or IP |
| `user` | str | ✅ Yes | - | Database username |
| `password` | str | ✅ Yes | - | Database password (in access_config) |
| `database` | str | ❌ No | None | Default database/schema for queries |
| `dbs_port` | int | ❌ No | 1025 | Database port number |

**Example with all options:**

```python
config = TeradataConnectionConfig(
    access_config=Secret(TeradataAccessConfig(password="pwd")),
    host="teradata.example.com",
    user="myuser",
    database="production_db",  # All queries use this database by default
    dbs_port=1025,             # Standard Teradata port
)
```

### Uploader Configuration (Destination)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `table_name` | str | "elements" | Target table name |
| `batch_size` | int | 50 | Records per batch insert |
| `record_id_key` | str | "record_id" | Column for identifying duplicate records |

### Indexer Configuration (Source)

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `table_name` | str | ✅ Yes | Source table name |
| `id_column` | str | ✅ Yes | Primary key or unique identifier column |
| `batch_size` | int | 100 | Number of records per batch |

### Downloader Configuration (Source)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `fields` | list[str] | [] | Specific columns to download (empty = all) |
| `download_dir` | str | Required | Directory to save downloaded data |

---

## 🗄️ Table Schema Requirements

### Destination Table

When creating a destination table for Unstructured elements, use this schema:

```sql
CREATE TABLE "elements" (
    "id" VARCHAR(64) NOT NULL,
    "record_id" VARCHAR(512),          -- ⚠️ IMPORTANT: Must be 512+ for long paths
    "element_id" VARCHAR(64),
    "text" VARCHAR(32000),
    "type" VARCHAR(50),                -- ⚠️ MUST be quoted (reserved word)
    "last_modified" TIMESTAMP,
    "languages" VARCHAR(1000),         -- JSON string
    "file_directory" VARCHAR(512),
    "filename" VARCHAR(256),
    "filetype" VARCHAR(100),
    "record_locator" VARCHAR(1000),    -- JSON string
    "date_created" TIMESTAMP,
    "date_modified" TIMESTAMP,
    "date_processed" TIMESTAMP,
    "permissions_data" VARCHAR(1000),  -- JSON string
    "filesize_bytes" INTEGER,
    "parent_id" VARCHAR(64),
    PRIMARY KEY ("id")
)
```

**Critical Notes:**

1. **`record_id` MUST be VARCHAR(512) or larger**
   - Stores full file paths (e.g., `/Users/user/Documents/path/to/file.txt`)
   - Using VARCHAR(64) will silently truncate paths and cause data loss
   - 512 is recommended, 1024 for very deep directory structures

2. **`"type"` column MUST be quoted**
   - `type` is a reserved word in Teradata
   - Without quotes, you'll get: `[Error 3707] Syntax error, expected something like a 'CHECK' keyword`

3. **JSON columns stored as VARCHAR**
   - `languages`, `permissions_data`, `record_locator` are JSON strings
   - The connector automatically converts Python lists/dicts to JSON

---

## ⚠️ Teradata-Specific Quirks & Gotchas

### 1. Reserved Words Require Quoting

Teradata has many reserved keywords that MUST be quoted when used as column names:

**Common Reserved Words:**
- `type` ⚠️ (used by Unstructured for element types)
- `year`
- `date`
- `user`
- `database`
- `current`
- `value`

**Solution:** Always quote column names in DDL:

```sql
-- ❌ WRONG (will fail)
CREATE TABLE elements (
    type VARCHAR(50),
    year INTEGER
)

-- ✅ CORRECT
CREATE TABLE "elements" (
    "type" VARCHAR(50),
    "year" INTEGER
)
```

**Good News:** This connector automatically quotes ALL identifiers in queries, so you're protected!

---

### 2. SQL Syntax Differences

Teradata uses different SQL syntax than MySQL/PostgreSQL:

| Feature | Standard SQL | Teradata SQL |
|---------|-------------|--------------|
| **Limit rows** | `SELECT ... LIMIT 10` | `SELECT TOP 10 ...` |
| **Current database** | `SELECT CURRENT_DATABASE()` | `SELECT DATABASE` |
| **Current user** | `SELECT CURRENT_USER()` | `SELECT USER` |

**Good News:** This connector handles these differences automatically!

---

### 3. Parameter Style: Question Marks

Teradata uses **qmark** paramstyle (`?` placeholders):

```python
# ✅ CORRECT (automatically done by connector)
cursor.execute(
    "INSERT INTO table (col1, col2) VALUES (?, ?)",
    ["value1", "value2"]
)

# ❌ WRONG (don't use %s or :name)
cursor.execute(
    "INSERT INTO table (col1, col2) VALUES (%s, %s)",  # Wrong!
    ["value1", "value2"]
)
```

**Good News:** This connector uses the correct parameter style automatically!

---

### 4. Data Type Conversions

The connector automatically handles these conversions:

| Python Type | Teradata Type | Notes |
|------------|---------------|-------|
| `str` | VARCHAR | Direct mapping |
| `int` | INTEGER | Direct mapping |
| `float` | FLOAT | Direct mapping |
| `datetime` | TIMESTAMP | Converted from ISO strings |
| `list` | VARCHAR | **Converted to JSON string** |
| `dict` | VARCHAR | **Converted to JSON string** |
| `None` | NULL | Direct mapping |

**Important:** Python lists and dicts are automatically converted to JSON strings because `teradatasql` driver is strict about types and cannot serialize these directly.

**Example:**

```python
# Input data
data = {
    "languages": ["eng", "fra"],           # Python list
    "metadata": {"key": "value"}           # Python dict
}

# Stored in Teradata as
{
    "languages": '["eng", "fra"]',         # JSON string
    "metadata": '{"key": "value"}'         # JSON string
}
```

---

### 5. VARCHAR Sizing Gotcha

**Problem:** Teradata silently truncates data if it exceeds column width.

**Example of Silent Truncation:**

```sql
-- Table definition
CREATE TABLE test (path VARCHAR(64));

-- Insert 81-character path
INSERT INTO test VALUES ('/Users/user/Documents/very/long/path/to/file.txt');

-- What gets stored (SILENTLY TRUNCATED!)
SELECT path FROM test;
-- Result: '/Users/user/Documents/very/long/path/to/file.txt' (only 64 chars)
```

**Solution:** Size your VARCHAR columns generously:

```sql
-- ❌ TOO SMALL
"record_id" VARCHAR(64)    -- Will truncate long paths

-- ✅ RECOMMENDED  
"record_id" VARCHAR(512)   -- Handles most file paths

-- ✅ SAFEST
"record_id" VARCHAR(1024)  -- Maximum safety
```

**How to Check for Truncation:**

```sql
SELECT 
    "record_id",
    CHARACTER_LENGTH("record_id") as stored_length
FROM "elements"
WHERE CHARACTER_LENGTH("record_id") < 100  -- Suspiciously short
```

---

### 6. Upsert Behavior

The connector implements upsert as **delete-then-insert**:

1. Delete all existing records with matching `record_id`
2. Insert new records

```python
# Automatic upsert on each upload
uploader.run(file_data)

# Behind the scenes:
# 1. DELETE FROM elements WHERE record_id = ?
# 2. INSERT INTO elements (...) VALUES (...)
```

**Important:** Make sure `record_id` column can hold full paths (see Quirk #5) to avoid upsert conflicts from truncated IDs.

---

## 🐛 Troubleshooting

### Error: "Syntax error, expected something like a 'CHECK' keyword"

**Cause:** Using reserved word as unquoted column name (often `type`)

**Solution:** Quote the column name in your CREATE TABLE:

```sql
-- Change this:
CREATE TABLE elements (type VARCHAR(50))

-- To this:
CREATE TABLE "elements" ("type" VARCHAR(50))
```

---

### Error: "Syntax error: expected something between 'table' and 'LIMIT' keyword"

**Cause:** Using `LIMIT` clause (not supported in Teradata)

**Solution:** Use `TOP` instead:

```sql
-- Change this:
SELECT * FROM table LIMIT 10

-- To this:
SELECT TOP 10 * FROM table
```

**Good News:** This connector uses `TOP` automatically!

---

### Error: "seqOfParams[0][4] unexpected type <class 'list'>"

**Cause:** Trying to insert Python list/dict without converting to JSON

**Solution:** The connector handles this automatically, but if you see this error, it means the `conform_dataframe()` method isn't being called.

**Good News:** This connector automatically converts lists/dicts to JSON strings!

---

### Issue: record_id values are truncated

**Symptoms:** File paths are cut off (e.g., `/Users/user/Documents/path/to/file.txt` becomes `/Users/user/Documents/path/to/fil`)

**Cause:** `record_id` column is VARCHAR(64) but paths are longer

**Diagnosis:**

```sql
SELECT 
    "record_id",
    CHARACTER_LENGTH("record_id") as length
FROM "elements"
LIMIT 5
```

If length is consistently 64 (or some other fixed number less than expected), you have truncation.

**Solution:** Recreate table with larger VARCHAR:

```sql
DROP TABLE "elements";
CREATE TABLE "elements" (
    "record_id" VARCHAR(512),  -- Increased from 64
    -- ... other columns ...
);
```

Then re-upload your data.

---

### Error: "Cannot connect to server"

**Cause:** Network/firewall issues, or wrong host/port

**Checklist:**
1. ✅ Can you ping the host?
2. ✅ Is the port correct? (default: 1025)
3. ✅ Are you using the full hostname? (e.g., `host-12345.env.clearscape.teradata.com`)
4. ✅ Is your firewall allowing connections?

---

### Error: "Access rights violation"

**Cause:** User lacks permissions

**Required Permissions:**
- **Source:** SELECT on source table
- **Destination:** SELECT, INSERT, DELETE on destination table

---

## 🔧 Advanced Usage

### Custom Batch Sizes

Optimize for your network and data size:

```python
# Small batches (safer for large rows)
uploader_config=TeradataUploaderConfig(
    table_name="elements",
    batch_size=10,  # 10 records per batch
)

# Large batches (faster for small rows)
uploader_config=TeradataUploaderConfig(
    table_name="elements",
    batch_size=500,  # 500 records per batch
)
```

---

### Field Selection (Source)

Download only specific columns to save bandwidth:

```python
downloader_config=TeradataDownloaderConfig(
    download_dir="./downloads",
    fields=["id", "text", "type"],  # Only these columns
)
```

---

### Custom Record ID Column

Use a different column for upsert identification:

```python
uploader_config=TeradataUploaderConfig(
    table_name="elements",
    record_id_key="document_path",  # Custom column name
)
```

**Important:** This column must also be VARCHAR(512)+ if it stores paths!

---

### Environment Variables

Store credentials securely:

```bash
# .env file
TERADATA_HOST=your-host.env.clearscape.teradata.com
TERADATA_USER=demo_user
TERADATA_PASSWORD=your_secure_password
TERADATA_DATABASE=production_db
```

```python
import os
from dotenv import load_dotenv

load_dotenv()

config = TeradataConnectionConfig(
    access_config=Secret(
        TeradataAccessConfig(password=os.getenv("TERADATA_PASSWORD"))
    ),
    host=os.getenv("TERADATA_HOST"),
    user=os.getenv("TERADATA_USER"),
    database=os.getenv("TERADATA_DATABASE"),
)
```

---

## 📊 Performance Tips

### 1. Use Indexed Columns

For source operations, ensure `id_column` is indexed:

```sql
CREATE INDEX idx_doc_id ON source_table(doc_id);
```

### 2. Optimize Batch Size

- **Small rows (<1KB):** batch_size=100-500
- **Medium rows (1-10KB):** batch_size=50-100
- **Large rows (>10KB):** batch_size=10-50

### 3. Network Latency

For high-latency connections:
- Increase batch_size to reduce round trips
- Consider running ingest job closer to Teradata instance

### 4. Monitor Query Performance

```sql
-- Check slow queries
SELECT QueryID, StartTime, TotalIOCount
FROM DBC.QryLogV
WHERE UserName = 'your_user'
ORDER BY StartTime DESC;
```

---

## 🔍 Debugging

### Enable Verbose Logging

```python
Pipeline.from_configs(
    context=ProcessorConfig(
        work_dir="./workdir",
        verbose=True,  # ← Detailed logging
    ),
    # ... rest of config
)
```

### Check Pipeline Stages

The connector logs each stage:

```
2025-11-13 10:03:05 INFO created indexer with configs: {...}
2025-11-13 10:03:05 INFO Created download with configs: {...}
2025-11-13 10:03:05 INFO created partition with configs: {...}
2025-11-13 10:03:05 INFO created upload_stage with configs: {...}
2025-11-13 10:03:05 INFO Created upload with configs: {...}
```

### Inspect SQL Queries

Look for `DEBUG` level logs:

```
2025-11-13 10:03:05 DEBUG running query: SELECT TOP 1 * FROM "elements"
2025-11-13 10:03:05 DEBUG running query: DELETE FROM "elements" WHERE "record_id" = ?
2025-11-13 10:03:05 DEBUG running query: INSERT INTO "elements" (...) VALUES(?,?,?)
```

---

## 📚 Additional Resources

### Official Documentation

- **Teradata SQL Driver:** https://github.com/Teradata/python-driver
- **Teradata Docs:** https://docs.teradata.com/
- **Unstructured Platform:** https://unstructured.io/

### Related Connectors

Similar SQL connectors in this codebase:
- `postgres.py` - PostgreSQL connector
- `singlestore.py` - SingleStore connector
- `snowflake.py` - Snowflake connector

### Implementation Details

See these files for technical details:
- `TERADATA_IMPLEMENTATION_PLAN.md` - Full implementation plan
- `PHASE2_USAGE_EXAMPLES.md` - Extended usage examples
- `QUICK_FIXES.md` - Development notes and bug fixes

---

## 🤝 Contributing

Found a bug or have a feature request? Please file an issue!

### Known Limitations

- ✅ Authentication: Only username/password (TD2) supported
- ⏳ LDAP/Kerberos: Not yet implemented (Phase 3)
- ⏳ FastLoad/MultiLoad: Not yet implemented
- ⏳ Connection pooling: Not yet implemented

---

## 📝 Changelog

### Phase 2 (Current)
- ✅ Added `database` parameter for default schema selection
- ✅ Added `dbs_port` parameter for custom port configuration
- ✅ Fixed reserved word handling (automatic identifier quoting)
- ✅ Fixed dynamic list/dict detection and JSON conversion
- ✅ Fixed DELETE statement quoting for bulletproof upsert
- ✅ Production tested with live Teradata Vantage Cloud

### Phase 1 (Initial Release)
- ✅ Basic username/password authentication
- ✅ Source and destination support
- ✅ Batch processing
- ✅ Automatic data type conversions
- ✅ Upsert behavior (delete-then-insert)

---

## 🎯 Quick Reference

### Most Common Commands

```python
# Destination: Local → Teradata
Pipeline.from_configs(
    indexer_config=LocalIndexerConfig(input_path="./docs"),
    uploader_config=TeradataUploaderConfig(table_name="elements"),
    destination_connection_config=TeradataConnectionConfig(...),
).run()

# Source: Teradata → Local
Pipeline.from_configs(
    indexer_config=TeradataIndexerConfig(table_name="source", id_column="id"),
    uploader_config=LocalUploaderConfig(output_dir="./output"),
    source_connection_config=TeradataConnectionConfig(...),
).run()
```

### Most Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| Syntax error with `type` | Reserved word | Quote it: `"type"` |
| Truncated record_id | VARCHAR(64) too small | Use VARCHAR(512) |
| `LIMIT` error | Wrong syntax | Use `TOP` (auto-handled) |
| List insertion fails | Wrong type | Auto-converted to JSON |

---

## ✅ Final Checklist

Before going to production:

- [ ] Table created with **quoted** `"type"` column
- [ ] `record_id` is **VARCHAR(512)** or larger
- [ ] User has **SELECT, INSERT, DELETE** permissions
- [ ] Connection parameters tested (`host`, `user`, `password`)
- [ ] Batch size optimized for your data
- [ ] Credentials stored securely (environment variables)
- [ ] Error handling tested (network failures, data issues)
- [ ] Verified full paths in `record_id` (no truncation)

---

**🚀 Your Teradata connector is production-ready! Happy ingesting!**

