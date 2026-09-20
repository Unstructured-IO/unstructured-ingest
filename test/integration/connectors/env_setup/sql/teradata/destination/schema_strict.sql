-- This table exists to REFUSE. Two deliberate narrowings, each chosen because the server
-- rejects it at INSERT time and the connector cannot quietly sanitise it away:
--   UNIQUE PRIMARY INDEX     -> a repeated "id" in one batch raises [Error 2801].
--   text CHARACTER SET LATIN -> a character outside LATIN raises [Error 6706].
-- Do not widen either one; the rejection IS the fixture.
CREATE MULTISET TABLE "elements_strict"
(
    "id"         VARCHAR(256) NOT NULL,
    "record_id"  VARCHAR(1024) NOT NULL,
    "element_id" VARCHAR(256) NOT NULL,
    "text"       VARCHAR(2048) CHARACTER SET LATIN,
    "type"       VARCHAR(256),
    "metadata"   JSON
)
UNIQUE PRIMARY INDEX ("id");
