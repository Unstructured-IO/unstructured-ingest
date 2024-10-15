import subprocess
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from unstructured_ingest.v2.interfaces import Downloader, FileData, Indexer

SOURCE_TAG = "source"
DESTINATION_TAG = "destination"

env_setup_path = Path(__file__).parents[3] / "test_e2e" / "env_setup"


@dataclass
class ValidationConfigs:
    expected_num_files: Optional[int] = None
    predownload_filedata_check: Optional[Callable[[FileData], None]] = None
    postdownload_filedata_check: Optional[Callable[[FileData], None]] = None


async def source_connector_validation(
    indexer: Indexer, downloader: Downloader, configs: ValidationConfigs
) -> None:
    for file_data in indexer.run():
        assert file_data
        if predownload_filedata_check := configs.predownload_filedata_check:
            predownload_filedata_check(file_data)
        if downloader.is_async():
            resp = await downloader.run_async(file_data=file_data)
        else:
            resp = downloader.run(file_data=file_data)
        postdownload_file_data = resp["file_data"]
        if postdownload_filedata_check := configs.postdownload_filedata_check:
            postdownload_filedata_check(postdownload_file_data)
    download_dir = downloader.download_config.download_dir
    downloaded_files = [p for p in download_dir.rglob("*") if p.is_file()]
    if expected_num_files := configs.expected_num_files:
        assert len(downloaded_files) == expected_num_files


@contextmanager
def docker_compose_context(docker_compose_path: Path):
    assert docker_compose_path.exists()
    if docker_compose_path.is_dir():
        if (docker_compose_path / "docker-compose.yml").exists():
            docker_compose_path = docker_compose_path / "docker-compose.yml"
        elif (docker_compose_path / "docker-compose.yaml").exists():
            docker_compose_path = docker_compose_path / "docker-compose.yaml"
    assert docker_compose_path.is_file()
    resp = None
    try:
        cmd = f"docker compose -f {docker_compose_path.resolve()} up -d --wait"
        print(f"Running command: {cmd}")
        resp = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
        )
        # Return code from docker compose using --wait can be 1 even if no error
        yield
    except Exception as e:
        if resp:
            print("STDOUT: {}".format(resp.stdout.decode("utf-8")))
            print("STDERR: {}".format(resp.stderr.decode("utf-8")))
        raise e
    finally:
        cmd = f"docker compose -f {docker_compose_path.resolve()} down --remove-orphans -v"
        print(f"Running command: {cmd}")
        final_resp = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
        )
        if final_resp.returncode != 0:
            print("STDOUT: {}".format(final_resp.stdout.decode("utf-8")))
            print("STDERR: {}".format(final_resp.stderr.decode("utf-8")))
