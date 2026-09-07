"""A run that moved zero files must say WHICH zero it hit.

Three different causes produce the same visible outcome today -- a job that indexed nothing:
the listing came back empty, the listing came back full but the indexer's own size/type
filter rejected every entry, or the configured glob/size filters dropped everything after
indexing. Only the pre-filter counts separate them, and until now they were computed inline
and thrown away, so the difference existed only in a debug log line nobody has when a
customer asks.

These tests pin the counts onto the OTel span the work is already running inside -- the
structured per-run channel the platform already points somewhere via
`ProcessorConfig.otel_endpoint` / `OTEL_EXPORTER_OTLP_ENDPOINT`. The consolidated WARNING that
accompanies each zero case follows the end-of-run summary this repo already uses for a
silently incomplete crawl (see the SharePoint Teams channel-skip summary).
"""

import logging
from contextlib import contextmanager
from unittest import mock

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.interfaces import ProcessorConfig
from unstructured_ingest.pipeline.pipeline import Pipeline
from unstructured_ingest.pipeline.steps.filter import FilterStep
from unstructured_ingest.processes.connectors.fsspec.fsspec import (
    FsspecConnectionConfig,
    FsspecIndexer,
    FsspecIndexerConfig,
)
from unstructured_ingest.processes.filter import Filterer, FiltererConfig


def warnings_matching(caplog, fragment: str) -> list[str]:
    """WARNING lines containing `fragment`.

    The span attributes are only readable through a collector; these sentences are what a
    person actually sees, and they are the whole point of the change. Assert on them.
    """
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING and fragment in r.getMessage()
    ]


class SpanRecorder:
    """Captures attributes off whatever span the code under test writes to.

    A local provider is used rather than the global one: `trace.get_current_span()` reads the
    context, not the provider, so the code under test finds this span without the test having
    to mutate global tracer state that other tests share.
    """

    def __init__(self):
        self.exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        self.tracer = provider.get_tracer("zero-file-diagnostics-test")

    @contextmanager
    def recording(self, name: str = "ingest process"):
        with self.tracer.start_as_current_span(name):
            yield

    @property
    def attributes(self) -> dict:
        merged = {}
        for span in self.exporter.get_finished_spans():
            merged.update(dict(span.attributes or {}))
        return merged


def build_indexer(listing: list[dict]) -> FsspecIndexer:
    connection_config = mock.MagicMock(spec=FsspecConnectionConfig)
    client = mock.MagicMock()
    client.ls.return_value = listing
    connection_config.get_client.return_value.__enter__.return_value = client
    connection_config.get_client.return_value.__exit__.return_value = None
    return FsspecIndexer(
        connection_config=connection_config,
        index_config=FsspecIndexerConfig(remote_url="s3://bucket/prefix", recursive=False),
    )


class TestIndexerReportsWhatTheListingReturned:
    def test_an_empty_listing_is_reported_as_an_empty_listing(self, caplog):
        recorder = SpanRecorder()
        indexer = build_indexer(listing=[])

        with caplog.at_level(logging.WARNING, logger="unstructured_ingest"), recorder.recording():
            assert indexer.get_file_info() == []

        assert recorder.attributes["source.listing.returned"] == 0
        assert recorder.attributes["source.listing.retained"] == 0
        # The span counts are useless without a collector. Pin the sentence too: it is the
        # half of this change a human reads, and swapping it for the other arm's text is
        # otherwise invisible to the suite.
        assert warnings_matching(caplog, "the listing returned no entries")
        assert not warnings_matching(caplog, "none of them are files with content")

    def test_a_listing_our_own_filter_empties_is_reported_as_that_instead(self, caplog):
        """The discriminating case: the source is NOT empty, our filter is what emptied it.

        Directory markers and zero-byte keys list fine and then fail `size > 0 and
        type == "file"`. Collapsed to a single "0 files indexed" this is byte-identical to an
        empty bucket, which is exactly the question the customer could not get answered.
        """
        recorder = SpanRecorder()
        indexer = build_indexer(
            listing=[
                {"name": "bucket/prefix/sub", "size": 0, "type": "directory"},
                {"name": "bucket/prefix/marker", "size": 0, "type": "file"},
                {"name": "bucket/prefix/other", "size": 0, "type": "directory"},
            ]
        )

        with caplog.at_level(logging.WARNING, logger="unstructured_ingest"), recorder.recording():
            assert indexer.get_file_info() == []

        assert recorder.attributes["source.listing.returned"] == 3
        assert recorder.attributes["source.listing.retained"] == 0
        # This is the discriminating sentence. If it were swapped for the empty-listing arm's
        # text the reader would be sent to check a path that is in fact populated.
        assert warnings_matching(caplog, "none of them are files with content")
        assert warnings_matching(caplog, "the listing returned 3 entries")
        assert not warnings_matching(caplog, "the listing returned no entries")

    def test_both_counts_are_reported_on_a_normal_run(self, caplog):
        recorder = SpanRecorder()
        indexer = build_indexer(
            listing=[
                {"name": "bucket/prefix/a.txt", "size": 10, "type": "file"},
                {"name": "bucket/prefix/b.txt", "size": 20, "type": "file"},
                {"name": "bucket/prefix/sub", "size": 0, "type": "directory"},
            ]
        )

        with caplog.at_level(logging.WARNING, logger="unstructured_ingest"), recorder.recording():
            assert len(indexer.get_file_info()) == 2

        assert recorder.attributes["source.listing.returned"] == 3
        assert recorder.attributes["source.listing.retained"] == 2
        # Reporting-only means silent on a run that worked.
        assert not warnings_matching(caplog, "nothing to index")


def write_file_data(tmp_path, name: str) -> str:
    file_data = FileData(
        identifier=name,
        connector_type="local",
        source_identifiers=SourceIdentifiers(filename=name, fullpath=f"/data/{name}"),
    )
    path = tmp_path / f"{name}.json"
    path.write_text(file_data.model_dump_json())
    return str(path)


def build_filter_step(tmp_path, file_glob: list[str]) -> FilterStep:
    return FilterStep(
        process=Filterer(config=FiltererConfig(file_glob=file_glob)),
        context=ProcessorConfig(work_dir=str(tmp_path), disable_parallelism=True),
    )


class TestFilterStepReportsWhatItDropped:
    @pytest.fixture
    def indexed_files(self, tmp_path) -> list[dict]:
        return [{"file_data_path": write_file_data(tmp_path, name)} for name in ("a.txt", "b.txt")]

    def test_filters_dropping_everything_is_reported(self, tmp_path, indexed_files, caplog):
        recorder = SpanRecorder()
        step = build_filter_step(tmp_path, file_glob=["*.pdf"])

        with caplog.at_level(logging.WARNING, logger="unstructured_ingest"), recorder.recording():
            results = step(indexed_files)

        assert [r for r in results if r] == []
        assert recorder.attributes["filter.indexed.received"] == 2
        assert recorder.attributes["filter.indexed.retained"] == 0
        assert warnings_matching(caplog, "none of the 2 records survived this stage")
        assert warnings_matching(caplog, "after filtering indexed content")

    def test_a_partial_drop_reports_both_counts(self, tmp_path, indexed_files, caplog):
        recorder = SpanRecorder()
        step = build_filter_step(tmp_path, file_glob=["*a.txt"])

        with caplog.at_level(logging.WARNING, logger="unstructured_ingest"), recorder.recording():
            results = step(indexed_files)

        assert len([r for r in results if r]) == 1
        assert recorder.attributes["filter.indexed.received"] == 2
        assert recorder.attributes["filter.indexed.retained"] == 1
        assert not warnings_matching(caplog, "survived this stage")

    def test_the_post_download_pass_is_reported_separately(self, tmp_path, indexed_files):
        """One Filterer runs at up to three stages; one set of keys would double-count."""
        recorder = SpanRecorder()
        step = build_filter_step(tmp_path, file_glob=["*.pdf"])

        with recorder.recording():
            step(indexed_files, stage="downloaded")

        assert recorder.attributes["filter.downloaded.received"] == 2
        assert recorder.attributes["filter.downloaded.retained"] == 0
        assert "filter.indexed.received" not in recorder.attributes


class TestUncompressExtractingNothingDoesNotCrash:
    """clean_results returns None, not [], when everything it is handed is falsy.

    The download path guards for that; the uncompress path went straight from clean_results
    into apply_filter. With a filterer configured and every archive extracting to zero
    regular files, apply_filter's list comprehension iterated None and the run died with a
    bare `TypeError: 'NoneType' object is not iterable` -- on the exact "everything got
    dropped" path this change exists to name. Pre-existing on main; fixed here because the
    call site is one of the lines this change edits.
    """

    def _pipeline(self, uncompress_output):
        pipeline = Pipeline.__new__(Pipeline)
        pipeline.context = ProcessorConfig(disable_parallelism=True)
        record = {"file_data_path": "/nonexistent/a.json"}
        pipeline.indexer_step = mock.MagicMock()
        pipeline.downloader_step = mock.MagicMock(return_value=[record])
        pipeline.uncompress_step = mock.MagicMock(return_value=uncompress_output)
        pipeline.partitioner_step = mock.MagicMock()
        pipeline.chunker_step = None
        pipeline.embedder_step = None
        pipeline.stager_step = None
        pipeline.uploader_step = mock.MagicMock()
        # A filterer IS configured: without one apply_filter returns None harmlessly and the
        # crash never fires, which is why this went unnoticed.
        pipeline.filter_step = mock.MagicMock(side_effect=lambda records, stage: records)
        return pipeline, record

    def test_zero_extracted_files_exits_instead_of_raising_typeerror(self, caplog):
        pipeline, record = self._pipeline(uncompress_output=[[]])

        with (
            caplog.at_level(logging.INFO, logger="unstructured_ingest"),
            mock.patch.object(Pipeline, "get_indices", return_value=[record]),
        ):
            pipeline._run()

        assert pipeline.partitioner_step.call_count == 0
        messages = [r.getMessage() for r in caplog.records]
        assert any("after uncompressing" in m for m in messages), messages
        # And the filter must not be blamed for a zero it did not produce.
        assert not any("after filtering uncompressed content" in m for m in messages), messages
        assert pipeline.filter_step.call_count == 2  # indexed + downloaded, not uncompressed
