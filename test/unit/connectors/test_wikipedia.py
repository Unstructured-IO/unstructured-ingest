import os
import time
from pathlib import Path
import pytest
from unittest.mock import Mock, patch

from unstructured_ingest.connector.wikipedia import (
    SimpleWikipediaConfig,
    WikipediaIngestTextDoc,
    WikipediaIngestHTMLDoc,
    WikipediaIngestSummaryDoc,
    WikipediaSourceConnector,
)
from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.interfaces import ProcessorConfig, ReadConfig


@pytest.fixture(autouse=True)
def setup_dirs():
    # Create temp directories for testing
    Path("/tmp/download").mkdir(exist_ok=True, parents=True)
    Path("/tmp/output").mkdir(exist_ok=True, parents=True)
    yield
    # Cleanup
    for file in Path("/tmp/download").glob("*"):
        file.unlink()
    for file in Path("/tmp/output").glob("*"):
        file.unlink()


@pytest.fixture
def mock_wiki_page():
    page = Mock()
    page.content = "Test content"
    page.html = lambda: "<html>Test content</html>"
    page.summary = "Test summary"
    page.url = "https://en.wikipedia.org/wiki/Test"
    page.revision_id = "123456"
    return page


@pytest.fixture
def config():
    return SimpleWikipediaConfig(page_title="Test Page")


@pytest.fixture
def read_config():
    return ReadConfig(download_dir="/tmp/download")


@pytest.fixture
def processor_config():
    return ProcessorConfig(output_dir="/tmp/output")


def test_wikipedia_config():
    config = SimpleWikipediaConfig(page_title="Test Page")
    assert config.page_title == "Test Page"
    assert config.auto_suggest is False
    assert config.request_delay == 1.0

    config = SimpleWikipediaConfig(page_title="Test Page", auto_suggest=True, request_delay=2.0)
    assert config.auto_suggest is True
    assert config.request_delay == 2.0


@patch("wikipedia.page")
def test_wikipedia_text_doc(mock_page, mock_wiki_page, config, read_config, processor_config):
    mock_page.return_value = mock_wiki_page
    
    doc = WikipediaIngestTextDoc(
        processor_config=processor_config,
        connector_config=config,
        read_config=read_config,
    )
    
    assert doc.text == "Test content"
    assert doc.get_filename_prefix() == "Test-Page"
    assert str(doc.filename).replace("/private", "") == "/tmp/download/Test-Page.txt"
    assert str(doc._output_filename) == "/tmp/output/Test-Page-txt.json"


@patch("wikipedia.page")
def test_wikipedia_html_doc(mock_page, mock_wiki_page, config, read_config, processor_config):
    mock_page.return_value = mock_wiki_page
    
    doc = WikipediaIngestHTMLDoc(
        processor_config=processor_config,
        connector_config=config,
        read_config=read_config,
    )
    
    assert doc.text == "<html>Test content</html>"
    assert str(doc.filename).replace("/private", "") == "/tmp/download/Test-Page.html"
    assert str(doc._output_filename) == "/tmp/output/Test-Page-html.json"


@patch("wikipedia.page")
def test_wikipedia_summary_doc(mock_page, mock_wiki_page, config, read_config, processor_config):
    mock_page.return_value = mock_wiki_page
    
    doc = WikipediaIngestSummaryDoc(
        processor_config=processor_config,
        connector_config=config,
        read_config=read_config,
    )
    
    assert doc.text == "Test summary"
    assert str(doc.filename).replace("/private", "") == "/tmp/download/Test-Page-summary.txt"
    assert str(doc._output_filename) == "/tmp/output/Test-Page-summary.json"


@patch("wikipedia.page")
def test_source_metadata(mock_page, mock_wiki_page, config, read_config, processor_config):
    mock_page.return_value = mock_wiki_page
    
    doc = WikipediaIngestTextDoc(
        processor_config=processor_config,
        connector_config=config,
        read_config=read_config,
    )
    
    doc.update_source_metadata()
    assert doc.source_metadata.exists is True
    assert doc.source_metadata.version == "123456"
    assert doc.source_metadata.source_url == "https://en.wikipedia.org/wiki/Test"


@patch("wikipedia.page")
def test_wikipedia_connector(mock_page, mock_wiki_page, config, read_config, processor_config):
    mock_page.return_value = mock_wiki_page
    
    connector = WikipediaSourceConnector(
        connector_config=config,
        processor_config=processor_config,
        read_config=read_config,
    )
    
    docs = connector.get_ingest_docs()
    assert len(docs) == 3
    assert isinstance(docs[0], WikipediaIngestTextDoc)
    assert isinstance(docs[1], WikipediaIngestHTMLDoc)
    assert isinstance(docs[2], WikipediaIngestSummaryDoc)


@patch("wikipedia.page")
def test_connection_error(mock_page, config, read_config, processor_config):
    mock_page.side_effect = Exception("Connection failed")
    
    connector = WikipediaSourceConnector(
        connector_config=config,
        processor_config=processor_config,
        read_config=read_config,
    )
    
    with pytest.raises(SourceConnectionError):
        connector.check_connection()


@patch("wikipedia.page")
def test_rate_limiting(mock_page, mock_wiki_page, config, read_config, processor_config):
    mock_page.return_value = mock_wiki_page
    
    # Set a 0.5 second delay between requests
    config = SimpleWikipediaConfig(page_title="Test Page", request_delay=0.5)
    
    doc = WikipediaIngestTextDoc(
        processor_config=processor_config,
        connector_config=config,
        read_config=read_config,
    )
    
    # First request should happen immediately
    start_time = time.time()
    _ = doc.page
    first_request_time = time.time() - start_time
    assert first_request_time < 0.1  # Should be nearly instant
    
    # Second request should be delayed by ~0.5 seconds
    start_time = time.time()
    _ = doc.page
    second_request_time = time.time() - start_time
    assert 0.4 <= second_request_time <= 0.6  # Allow small timing variance