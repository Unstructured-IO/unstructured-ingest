CREATE TABLE IF NOT EXISTS `elements` (
    id STRING NOT NULL PRIMARY KEY,
    record_id STRING NOT NULL,
    element_id STRING NOT NULL,
    text STRING,
    embeddings ARRAY<FLOAT>,
    type STRING,
    metadata VARIANT
);
