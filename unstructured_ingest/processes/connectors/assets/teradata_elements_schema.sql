CREATE MULTISET TABLE "elements"
(
    "id"         VARCHAR(256) NOT NULL,
    "record_id"  VARCHAR(1024) NOT NULL,
    "element_id" VARCHAR(256) NOT NULL,
    "text"       CLOB CHARACTER SET UNICODE,
    "type"       VARCHAR(256),
    "metadata"   JSON
)
PRIMARY INDEX ("id");
