import io
import logging
from contextlib import contextmanager
from unittest.mock import patch

import pytest

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connectors.duckdb.motherduck import (
    MotherDuckAccessConfig,
    MotherDuckConnectionConfig,
    MotherDuckUploader,
    MotherDuckUploaderConfig,
)


def test_precheck_redacts_token():
    secret = "md_token_SUPERSECRETVALUE123"
    connection_config = MotherDuckConnectionConfig(
        database="db",
        access_config=MotherDuckAccessConfig(md_token=secret),
    )
    uploader = MotherDuckUploader(
        upload_config=MotherDuckUploaderConfig(),
        connection_config=connection_config,
    )

    @contextmanager
    def boom(self):
        raise Exception(f"OperationalError: md:?motherduck_token={secret}")
        yield

    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    logger.addHandler(handler)
    try:
        with (
            patch.object(MotherDuckConnectionConfig, "get_cursor", boom),
            pytest.raises(DestinationConnectionError) as exc_info,
        ):
            uploader.precheck()
        assert secret not in str(exc_info.value)
        assert secret not in buf.getvalue()
    finally:
        logger.removeHandler(handler)
