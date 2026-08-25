import pytest

# Importing the connectors package registers every entry via import-time side effects.
import unstructured_ingest.processes.connectors  # noqa: F401
from unstructured_ingest.processes.connector_registry import (
    LocationShape,
    SourceRegistryEntry,
    destination_registry,
    source_registry,
)

# fsspec sources that emit a per-record version (box and sftp do not).
VERSION_EMITTING_FSSPEC = ("s3", "azure", "gcs", "dropbox")
FSSPEC_COHORT = VERSION_EMITTING_FSSPEC + ("box", "sftp")


@pytest.mark.parametrize("connector_type", FSSPEC_COHORT)
def test_fsspec_source_entry_markers(connector_type):
    entry = source_registry[connector_type]
    assert entry.location_shape == LocationShape.FSSPEC_URL
    assert entry.location_identity == ("indexer_config.remote_url",)
    assert entry.emits_record_version == (connector_type in VERSION_EMITTING_FSSPEC)

    schema = entry.indexer_config.model_json_schema()
    assert schema["properties"]["remote_url"].get("x-runtime-eligible") is True
    assert schema["properties"]["recursive"].get("x-runtime-eligible") is True


@pytest.mark.parametrize("connector_type", FSSPEC_COHORT)
def test_fsspec_destination_entry_markers(connector_type):
    entry = destination_registry[connector_type]
    assert entry.location_shape == LocationShape.FSSPEC_URL
    assert entry.location_identity == ("uploader_config.remote_url",)

    schema = entry.uploader_config.model_json_schema()
    assert schema["properties"]["remote_url"].get("x-runtime-eligible") is True


def test_sql_table_source_entry_markers():
    # postgres represents the sql-table cohort: connection database + indexer table
    # compose the equality identity, no recursion, no record version.
    entry = source_registry["postgres"]
    assert entry.location_shape == LocationShape.SQL_TABLE
    assert entry.location_identity == (
        "connector_config.database",
        "indexer_config.table_name",
    )
    assert entry.supports_recursion is False
    assert entry.emits_record_version is False

    conn = entry.connection_config.model_json_schema()
    assert conn["properties"]["database"].get("x-runtime-eligible") is True
    idx = entry.indexer_config.model_json_schema()
    assert idx["properties"]["table_name"].get("x-runtime-eligible") is True


def test_sql_table_destination_entry_markers():
    # kdbai represents the destination-only sql-table cohort.
    entry = destination_registry["kdbai"]
    assert entry.location_shape == LocationShape.SQL_TABLE
    assert entry.location_identity == (
        "uploader_config.database_name",
        "uploader_config.table_name",
    )
    assert entry.supports_recursion is False

    up = entry.uploader_config.model_json_schema()
    assert up["properties"]["database_name"].get("x-runtime-eligible") is True
    assert up["properties"]["table_name"].get("x-runtime-eligible") is True


def test_sql_table_destination_write_target_markers():
    # postgres represents the dual-role sql-table cohort as a destination: it writes
    # to a connection database (fixed identity) and a runtime-eligible uploader table.
    entry = destination_registry["postgres"]
    assert entry.location_shape == LocationShape.SQL_TABLE
    assert entry.location_identity == (
        "connector_config.database",
        "uploader_config.table_name",
    )
    assert entry.supports_recursion is False
    assert entry.emits_record_version is False

    up = entry.uploader_config.model_json_schema()
    assert up["properties"]["table_name"].get("x-runtime-eligible") is True


def test_search_index_destination_entry_markers():
    # pinecone represents the search-index cohort: a connection-hosted index name,
    # equality identity, no recursion.
    entry = destination_registry["pinecone"]
    assert entry.location_shape == LocationShape.SEARCH_INDEX
    assert entry.location_identity == ("connector_config.index_name",)
    assert entry.supports_recursion is False

    conn = entry.connection_config.model_json_schema()
    assert conn["properties"]["index_name"].get("x-runtime-eligible") is True


def test_search_index_uploader_collection_marker():
    # weaviate-cloud keeps its collection on the uploader config; only that leaf is
    # runtime-eligible (the cluster_url is identity-only).
    entry = destination_registry["weaviate-cloud"]
    assert entry.location_shape == LocationShape.SEARCH_INDEX
    assert entry.location_identity == (
        "connector_config.cluster_url",
        "uploader_config.collection",
    )
    up = entry.uploader_config.model_json_schema()
    assert up["properties"]["collection"].get("x-runtime-eligible") is True


def test_api_folder_source_entry_markers():
    # sharepoint represents the api-folder cohort with a real folder tree: site
    # (connection) + path (indexer) identity, recursive traversal, record version.
    entry = source_registry["sharepoint"]
    assert entry.location_shape == LocationShape.API_FOLDER
    assert entry.location_identity == (
        "connector_config.site",
        "connector_config.library",
        "indexer_config.path",
    )
    assert entry.supports_recursion is True
    assert entry.emits_record_version is True

    conn = entry.connection_config.model_json_schema()
    assert conn["properties"]["site"].get("x-runtime-eligible") is True
    idx = entry.indexer_config.model_json_schema()
    assert idx["properties"]["path"].get("x-runtime-eligible") is True
    assert idx["properties"]["recursive"].get("x-runtime-eligible") is True


def test_api_folder_without_folder_tree_has_no_recursion():
    # gitlab is api-folder but a flat listing, so recursion is off.
    entry = source_registry["gitlab"]
    assert entry.location_shape == LocationShape.API_FOLDER
    assert entry.supports_recursion is False


def test_other_shape_source_entry_markers():
    # local represents the "other" cohort: an equality path with no folder tree.
    entry = source_registry["local"]
    assert entry.location_shape == LocationShape.OTHER
    assert entry.location_identity == ("indexer_config.input_path",)
    assert entry.supports_recursion is True

    idx = entry.indexer_config.model_json_schema()
    assert idx["properties"]["input_path"].get("x-runtime-eligible") is True
    assert idx["properties"]["recursive"].get("x-runtime-eligible") is True


def test_fsspec_url_delta_table_destination_marker():
    # delta_table is a destination-only fsspec-url shape (table_uri is an s3 URL).
    entry = destination_registry["delta_table"]
    assert entry.location_shape == LocationShape.FSSPEC_URL
    assert entry.location_identity == ("connector_config.table_uri",)
    assert entry.supports_recursion is True

    conn = entry.connection_config.model_json_schema()
    assert conn["properties"]["table_uri"].get("x-runtime-eligible") is True


def test_unannotated_entry_is_unmarked():
    # A connector that sets no markers reports location_shape None so consumers
    # fall back to their own defaults rather than deriving an fsspec identity.
    entry = SourceRegistryEntry(indexer=object, downloader=object)
    assert entry.location_shape is None
    assert entry.emits_record_version is False


def test_opensearch_dual_role_location_identity():
    # opensearch is dual-role: as a source the index_name reshapes onto the
    # indexer config, as a destination onto the uploader config. The identity of
    # each entry must name the section its own role actually reshapes into.
    source = source_registry["opensearch"]
    assert source.location_identity == (
        "connector_config.hosts",
        "indexer_config.index_name",
    )
    assert (
        source.indexer_config.model_json_schema()["properties"]["index_name"].get(
            "x-runtime-eligible"
        )
        is True
    )

    dest = destination_registry["opensearch"]
    assert dest.location_identity == (
        "connector_config.hosts",
        "uploader_config.index_name",
    )
    assert (
        dest.uploader_config.model_json_schema()["properties"]["index_name"].get(
            "x-runtime-eligible"
        )
        is True
    )


def _eligible(config) -> set[str]:
    props = config.model_json_schema().get("properties", {})
    return {name for name, prop in props.items() if prop.get("x-runtime-eligible") is True}


def _fields(config) -> set[str]:
    return set(config.model_json_schema().get("properties", {}).keys())


@pytest.mark.parametrize(
    "registry", [source_registry, destination_registry], ids=["source", "destination"]
)
def test_annotated_identity_leaves_match_eligible_markers(registry):
    # Invariant: every location_identity leaf of an annotated entry must be a real
    # field on the config class its section names, and carry x-runtime-eligible
    # unless it is a fixed connection-level identity field (connector_config.*).
    sections = {
        "connector_config": "connection_config",
        "indexer_config": "indexer_config",
        "uploader_config": "uploader_config",
    }
    for name, entry in registry.items():
        if entry.location_shape is None:
            continue
        for path in entry.location_identity:
            section, _, leaf = path.partition(".")
            config = getattr(entry, sections[section], None)
            assert config is not None, f"{name}: {path} names an absent section"
            assert leaf in _fields(config), f"{name}: {path} is not a real field"
            if section != "connector_config":
                assert leaf in _eligible(config), f"{name}: {path} is not runtime-eligible"


@pytest.mark.parametrize(
    "registry, default_identity",
    [
        (source_registry, ("indexer_config.remote_url",)),
        (destination_registry, ("uploader_config.remote_url",)),
    ],
    ids=["source", "destination"],
)
def test_unannotated_entries_are_ignored(registry, default_identity):
    # Opposite of test_annotated_identity_leaves_match_eligible_markers: parameterization
    # is gated solely on location_shape. An unannotated entry keeps the fsspec-looking
    # default location_identity and may even expose x-runtime-eligible fields through a
    # config class it shares with an annotated sibling (databricks_volumes_*, sqlite,
    # qdrant/weaviate variants), yet stays location_shape=None so consumers ignore it
    # rather than mistaking it for an fsspec target. Locks in "unannotated = ignored".
    unannotated = {name: e for name, e in registry.items() if e.location_shape is None}
    assert unannotated, "expected some unannotated entries to guard the ignore path"

    shares_eligible_config = False
    for name, entry in unannotated.items():
        # The default identity is fsspec-shaped, so anything keyed on location_identity
        # rather than location_shape would wrongly treat these as fsspec targets.
        assert entry.location_identity == default_identity, f"{name}: unexpected identity"
        assert entry.supports_recursion is True, f"{name}: recursion should stay default"
        assert entry.emits_record_version is False, f"{name}: version should stay default"
        for section in ("connection_config", "indexer_config", "uploader_config"):
            config = getattr(entry, section, None)
            if config is not None and _eligible(config):
                shares_eligible_config = True

    # Eligibility alone must not parameterize: at least one ignored entry exposes eligible
    # fields via a shared config class and stays ignored purely because its shape is None.
    assert shares_eligible_config
