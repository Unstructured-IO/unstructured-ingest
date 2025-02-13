import asyncio
import json
import os
from pathlib import Path
from typing import Optional

import numpy as np
import pytest

from zenpy import Zenpy, ZenpyException 

from test.integration.connectors.utils.validation.source import (
    SourceValidationConfigs,
    source_connector_validation,
)

from test.integration.connectors.utils.constants import DESTINATION_TAG, NOSQL_TAG
from test.integration.utils import requires_env
from unstructured_ingest.v2.interfaces.file_data import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.zendesk import (CONNECTOR_TYPE as ZENDESK_CONNECTOR_TYPE)


from unstructured_ingest.v2.processes.connectors.zendesk import (
    ZendeskConnectionConfig,
    ZendeskAccessConfig,
    ZendeskIndexerConfig, 
    ZendeskIndexer, 
    ZendeskDownloaderConfig, 
    ZendeskDownloader
)


def zendesk_source_test(
        tmp_path: Path, 
        token: Optional[str] = None,    
        endpoint: Optional[str] = None, 
        email: Optional[str] = None, 
        subdomain: Optional[str] = None, 
    ): 
    
    access_config = ZendeskAccessConfig(api_token=token)
    connection_config = ZendeskConnectionConfig(
        sub_domain = subdomain,
        email=email, 
        endpoint=endpoint,
        access_config = access_config)
    
    index_config = ZendeskIndexerConfig(batch_size=10)

    indexer = ZendeskIndexer(
        connection_config=connection_config, 
        index_config=index_config,
        connector_type = ZENDESK_CONNECTOR_TYPE,
    )

    # validate this garbage here 
    breakpoint() 

@requires_env("ZENDESK_ENDPOINT", "ZENDESK_TOKEN")
def test_zendesk_source(temp_dir):
    zendesk_source_test(tmp_path=temp_dir, 
                        token=os.environ["ZENDESK_TOKEN"],
                        endpoint=os.environ["ZENDESK_ENDPOINT"],
                        email="test@unstructured.io",
                        subdomain="unstructuredhelp")
