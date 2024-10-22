from unstructured_ingest.v2.processes.connectors.fsspec.fsspec import (
    FsspecIndexer,
)


def test_fsspec_indexer_sampling_happy_path():

    indexer = FsspecIndexer(
        connection_config="fake_connection_config",
        index_config="fake_index_config",
        connector_type="fake_connector_type",
    )

    all_files = [{"name": "fake_file.txt"}, {"name": "fake_file2.txt"}, {"name": "fake_file3.txt"}]

    sampled_files = indexer.sample_n_files(all_files, 2)
    assert len(sampled_files) == 2
    for sampled_file in sampled_files:
        assert type(sampled_file) == dict  # noqa: E721
        assert sampled_file["name"] in [file["name"] for file in all_files]


def test_fsspec_indexer_sampling_no_files():
    indexer = FsspecIndexer(
        connection_config="fake_connection_config",
        index_config="fake_index_config",
        connector_type="fake_connector_type",
    )

    all_files = []

    sampled_files = indexer.sample_n_files(all_files, 2)
    assert len(sampled_files) == 0


def test_fsspec_indexer_sampling_bigger_sample_size():
    indexer = FsspecIndexer(
        connection_config="fake_connection_config",
        index_config="fake_index_config",
        connector_type="fake_connector_type",
    )

    all_files = [{"name": "fake_file.txt"}, {"name": "fake_file2.txt"}, {"name": "fake_file3.txt"}]

    sampled_files = indexer.sample_n_files(all_files, 10)
    assert len(sampled_files) == 3
    for sampled_file in sampled_files:
        assert type(sampled_file) == dict  # noqa: E721
        assert sampled_file["name"] in [file["name"] for file in all_files]
