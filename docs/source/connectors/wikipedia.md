# Wikipedia Connector

The Wikipedia connector allows you to fetch and process content from Wikipedia articles. It supports retrieving the full article text, HTML content, and article summaries.

## Installation

Install the package with the Wikipedia connector dependencies:

```bash
pip install "unstructured-ingest[wikipedia]"
```

## Configuration

The Wikipedia connector supports the following configuration options:

- `page_title` (required): The title of the Wikipedia page to fetch
- `auto_suggest` (optional, default=False): Whether to use Wikipedia's auto-suggestion for page titles
- `request_delay` (optional, default=1.0): Delay in seconds between API requests to avoid rate limiting

## Examples

### Basic Usage

```python
from unstructured_ingest.connector.wikipedia import SimpleWikipediaConfig, WikipediaSourceConnector

# Configure the connector
config = SimpleWikipediaConfig(
    page_title="Artificial Intelligence",
    auto_suggest=True,
    request_delay=1.0  # 1 second delay between requests
)

# Initialize the connector
connector = WikipediaSourceConnector(
    connector_config=config,
    processor_config=processor_config,
    read_config=read_config
)

# Get all ingest documents (text, HTML, and summary)
docs = connector.get_ingest_docs()

# Process each document
for doc in docs:
    # Each doc will be one of: WikipediaIngestTextDoc, WikipediaIngestHTMLDoc, or WikipediaIngestSummaryDoc
    content = doc.text  # Get the content
    filename = doc.filename  # Get the output filename
```

### Output Files

The connector creates three files for each Wikipedia article:

1. `{title}.txt` - The full article text content
2. `{title}.html` - The HTML version of the article
3. `{title}-summary.txt` - The article summary

The files are saved in the directory specified by `read_config.download_dir`.