import os
from pathlib import Path
from typing import Optional

import pytest

from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)
from test.integration.utils import requires_env
from unstructured_ingest.v2.processes.connectors.zendesk import (
    ZendeskAccessConfig,
    ZendeskConnectionConfig,
    ZendeskDownloader,
    ZendeskDownloaderConfig,
    ZendeskIndexer,
    ZendeskIndexerConfig,
)


async def zendesk_source_test(
    tmp_path: Path,
    token: Optional[str] = None,
    endpoint: Optional[str] = None,
    email: Optional[str] = None,
    subdomain: Optional[str] = None,
):

    access_config = ZendeskAccessConfig(api_token=token)
    connection_config = ZendeskConnectionConfig(
        subdomain=subdomain, email=email, endpoint=endpoint, access_config=access_config
    )

    index_config = ZendeskIndexerConfig(batch_size=1)

    indexer = ZendeskIndexer(
        connection_config=connection_config,
        index_config=index_config,
        connector_type="zendesk",
    )

    with connection_config.get_client() as client:
        tickets = client.get_tickets()
        client.get_users()
        for ticket in tickets:
            client.get_comments(ticket.id)

    # handle downloader.
    download_config = ZendeskDownloaderConfig(download_dir=tmp_path)

    downloader = ZendeskDownloader(
        connection_config=connection_config,
        download_config=download_config,
        connector_type="zendesk",
    )

    # test script 
    #TODO(Remove later)
    from unstructured_ingest.v2.processes.connectors.zendesk.wrapper import ZendeskClient


    with connection_config.get_client() as async_client:
        # tickets = await async_client.get_tickets_async()
        articles = async_client.get_articles()

    breakpoint() 

    # tickets = list( await indexer.run())

    # fdata = tickets[0]

    # result = await downloader.run(fdata)
    
    # # Run the source connector validation
    # await source_connector_validation(
    #     indexer=indexer,
    #     downloader=downloader,
    #     configs=SourceValidationConfigs(
    #         test_id="zendesk", expected_num_files=8, validate_file_data=False
    #     ),
    # )


@pytest.mark.asyncio
@requires_env("ZENDESK_ENDPOINT", "ZENDESK_TOKEN")
async def test_zendesk_source(temp_dir):
    await zendesk_source_test(
        tmp_path=temp_dir,
        token=os.environ["ZENDESK_TOKEN"],
        endpoint=os.environ["ZENDESK_ENDPOINT"],
        email="test@unstructured.io",
        subdomain="unstructuredhelp",
    )
