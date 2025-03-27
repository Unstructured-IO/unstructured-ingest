CREATE TABLE elements (
    id STRING NOT NULL PRIMARY KEY,
    record_id STRING,
    element_id STRING,
    text STRING,
    embeddings ARRAY<FLOAT>,
    type STRING,
    metadata VARIANT
);

