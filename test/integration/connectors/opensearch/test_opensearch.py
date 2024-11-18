import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import pandas as pd
import pytest
from opensearchpy import Document, Keyword, OpenSearch, Text

from test.integration.connectors.utils.constants import SOURCE_TAG
from test.integration.connectors.utils.docker import HealthCheck, container_context
from test.integration.connectors.utils.validation import (
    ValidationConfigs,
    source_connector_validation,
)
from unstructured_ingest.v2.processes.connectors.opensearch import (
    CONNECTOR_TYPE,
    OpenSearchAccessConfig,
    OpenSearchConnectionConfig,
    OpenSearchDownloader,
    OpenSearchDownloaderConfig,
    OpenSearchIndexer,
    OpenSearchIndexerConfig,
)

INDEX_NAME = "movies"


class Movie(Document):
    title = Text(fields={"raw": Keyword()})
    year = Text()
    director = Text()
    cast = Text()
    genre = Text()
    wiki_page = Text()
    ethnicity = Text()
    plot = Text()

    class Index:
        name = INDEX_NAME

    def save(self, **kwargs):
        return super(Movie, self).save(**kwargs)


@contextmanager
def get_client() -> Generator[OpenSearch, None, None]:
    with OpenSearch(
        hosts=[{"host": "localhost", "port": 9200}],
        http_auth=("admin", "admin"),
        use_ssl=True,
        verify_certs=False,
        ssl_show_warn=False,
    ) as client:
        yield client


@pytest.fixture
def source_index(movies_dataframe: pd.DataFrame) -> str:
    with container_context(
        image="opensearchproject/opensearch:2.11.1",
        ports={9200: 9200, 9600: 9600},
        environment={"discovery.type": "single-node"},
        healthcheck=HealthCheck(
            test="curl --fail https://localhost:9200/_cat/health -ku 'admin:admin' >/dev/null || exit 1",
            interval=1,
        ),
    ):
        with get_client() as client:
            Movie.init(using=client)
            for i, row in movies_dataframe.iterrows():
                movie = Movie(
                    meta={"id": i},
                    title=row["Title"],
                    year=row["Release Year"],
                    director=row["Director"],
                    cast=row["Cast"],
                    genre=row["Genre"],
                    wiki_page=row["Wiki Page"],
                    ethnicity=row["Origin/Ethnicity"],
                    plot=row["Plot"],
                )
                try:
                    movie.save(using=client)
                except Exception as e:
                    print(f"failed to save movie: {row}")
                    raise e
        yield INDEX_NAME


@pytest.mark.asyncio
@pytest.mark.tags(CONNECTOR_TYPE, SOURCE_TAG)
async def test_opensearch_source(source_index, movies_dataframe: pd.DataFrame):
    indexer_config = OpenSearchIndexerConfig(index_name=source_index)
    with tempfile.TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        connection_config = OpenSearchConnectionConfig(
            access_config=OpenSearchAccessConfig(password="admin"),
            username="admin",
            hosts=["http://localhost:9200"],
            use_ssl=True,
        )
        download_config = OpenSearchDownloaderConfig(download_dir=tempdir_path)
        indexer = OpenSearchIndexer(
            connection_config=connection_config, index_config=indexer_config
        )
        downloader = OpenSearchDownloader(
            connection_config=connection_config, download_config=download_config
        )
        expected_num_files = len(movies_dataframe)
        await source_connector_validation(
            indexer=indexer,
            downloader=downloader,
            configs=ValidationConfigs(
                test_id="opensearch",
                expected_num_files=expected_num_files,
            ),
        )
