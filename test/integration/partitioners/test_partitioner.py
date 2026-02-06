import os
from pathlib import Path

import pytest

from test.integration.utils import requires_env
from unstructured_ingest.error import UserError
from unstructured_ingest.processes.partitioner import Partitioner, PartitionerConfig

int_test_dir = Path(__file__).parent
assets_dir = int_test_dir / "assets"

# TODO: api currently does not support gz files anymore, add back in when that gets fixed
all_partition_files = [
    path
    for path in assets_dir.iterdir()
    if path.is_file()
    if path.name != "layout-parser-paper.pdf.gz"
]
non_image_partition_files = [
    path for path in all_partition_files if path.suffix not in [".jpg", ".png", ".tif"]
]
supported_fast_partition_files = [
    path for path in non_image_partition_files if path.suffix not in [".eml", ".epub"]
]
image_partition_files = [
    path for path in all_partition_files if path not in non_image_partition_files
]


@pytest.mark.parametrize(
    "partition_file", all_partition_files, ids=[path.name for path in all_partition_files]
)
@requires_env("UNSTRUCTURED_API_KEY", "UNSTRUCTURED_API_URL")
@pytest.mark.asyncio
# hi_res strategy can be slow for large/image-heavy files; 300s accommodates the
# full parametrized matrix without flaking on transient API latency spikes.
@pytest.mark.timeout(300)
async def test_partitioner_api_hi_res(partition_file: Path):
    api_key = os.getenv("UNSTRUCTURED_API_KEY")
    api_url = os.getenv("UNSTRUCTURED_API_URL")
    partitioner_config = PartitionerConfig(
        strategy="hi_res",
        partition_by_api=True,
        api_key=api_key,
        partition_endpoint=api_url,
        api_timeout_ms=10000,
    )
    partitioner = Partitioner(config=partitioner_config)
    results = await partitioner.run_async(filename=partition_file)
    assert results


@pytest.mark.parametrize(
    "partition_file",
    supported_fast_partition_files,
    ids=[path.name for path in supported_fast_partition_files],
)
@requires_env("UNSTRUCTURED_API_KEY", "UNSTRUCTURED_API_URL")
@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_partitioner_api_fast(partition_file: Path):
    api_key = os.getenv("UNSTRUCTURED_API_KEY")
    api_url = os.getenv("UNSTRUCTURED_API_URL")
    partitioner_config = PartitionerConfig(
        strategy="fast",
        partition_by_api=True,
        api_key=api_key,
        partition_endpoint=api_url,
    )
    partitioner = Partitioner(config=partitioner_config)
    results = await partitioner.run_async(filename=partition_file)
    assert results


@pytest.mark.parametrize(
    "partition_file", image_partition_files, ids=[path.name for path in image_partition_files]
)
@requires_env("UNSTRUCTURED_API_KEY", "UNSTRUCTURED_API_URL")
@pytest.mark.asyncio
async def test_partitioner_api_fast_error(partition_file: Path):
    api_key = os.getenv("UNSTRUCTURED_API_KEY")
    api_url = os.getenv("UNSTRUCTURED_API_URL")
    partitioner_config = PartitionerConfig(
        strategy="fast",
        partition_by_api=True,
        api_key=api_key,
        partition_endpoint=api_url,
        raise_unsupported_filetype=True,
    )
    partitioner = Partitioner(config=partitioner_config)
    with pytest.raises(UserError):
        await partitioner.run_async(filename=partition_file)
