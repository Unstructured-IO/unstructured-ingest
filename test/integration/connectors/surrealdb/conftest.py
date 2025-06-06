from pathlib import Path

import pytest
import subprocess
import tempfile
import os
import time
import json
import string
import random
from typing import Any, Generator

from unstructured_ingest.processes.connectors.surrealdb.surrealdb import (
    SurrealDBConnectionConfig,
    SurrealDBAccessConfig,
)

int_test_dir = Path(__file__).parent
assets_dir = int_test_dir / "assets"


REMOTE_CONFIG_PATH = "test/integration/connectors/surrealdb/assets/remote_config.json"


def pytest_generate_tests(metafunc):
    if "config" not in metafunc.fixturenames:
        return

    configs: list[str] = []
    # Check if surreal command is available
    try:
        # Note that the integration tests require the surreal command to be available
        # in the PATH.
        result = subprocess.run(
            ["surreal", "--version"], capture_output=True, text=True, check=False
        )
        if result.returncode == 0:
            configs.append("local_config")
        else:
            print(
                f"Skipping local SurrealDB tests because 'surreal' command not found or failed. Error: {result.stderr}"
            )
    except FileNotFoundError:
        print("Skipping local SurrealDB tests because 'surreal' command not found.")

    if Path(REMOTE_CONFIG_PATH).is_file():
        configs.append("remote_config")
    else:
        print(
            f"Skipping containerized SurrealDB tests because config file not found at: {REMOTE_CONFIG_PATH}"
        )

    # for test_name in ["test_check_succeeds", "test_write"]:
    metafunc.parametrize("config", configs, indirect=True)


@pytest.fixture(scope="module")
def test_namespace_name() -> str:
    letters = string.ascii_lowercase
    rand_string = "".join(random.choice(letters) for _ in range(6))
    return f"test_db_{rand_string}"


@pytest.fixture(scope="module")
def test_database_name() -> str:
    letters = string.ascii_lowercase
    rand_string = "".join(random.choice(letters) for _ in range(6))
    return f"test_db_{rand_string}"


@pytest.fixture
def config(
    request, test_namespace_name: str, test_database_name: str, surrealdb_schema: Path
) -> Generator[Any, Any, Any]:
    if request.param == "local_config":
        process = None
        tmp_dir = tempfile.TemporaryDirectory()
        try:
            db_dir_path = os.path.join(str(tmp_dir.name), "test.surrealdb")
            os.makedirs(db_dir_path, exist_ok=True)
            cmd = [
                "surreal",
                "start",
                "--allow-all",
                "--user",
                "root",
                "--pass",
                "root",
                "--log",
                "trace",  # Or "debug" for more verbose logs if needed
                "--bind",
                "0.0.0.0:8000",
                f"rocksdb://{db_dir_path}",
            ]

            process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

            # Wait for server to initialize.
            # A more robust check (e.g., trying to connect) could be added if necessary.
            time.sleep(3)

            if process.poll() is not None:
                # pylint: disable=C0301
                stderr_output = (
                    process.stderr.read().decode() if process.stderr else "No stderr output."
                )
                raise RuntimeError(
                    f"Failed to start SurrealDB server. Exit code: {process.returncode}. "
                    f"Command: {' '.join(cmd)}. Stderr: {stderr_output}"
                )

            conf = SurrealDBConnectionConfig(
                url="ws://localhost:8000",
                namespace=test_namespace_name,
                database=test_database_name,
                access_config=SurrealDBAccessConfig(
                    username="root",
                    password="root",
                ),
            )

            with conf.get_client() as client:
                client.query(f"DEFINE DATABASE {test_database_name};")
                client.query(f"DEFINE NAMESPACE {test_namespace_name};")
                client.use(test_namespace_name, test_database_name)
                with surrealdb_schema.open("r") as f:
                    query = f.read()
                client.query(query)

            yield conf

        finally:
            # Ensure process is terminated
            # and all resources are cleaned up
            if process:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                if process.stderr:
                    process.stderr.close()
            tmp_dir.cleanup()

    elif request.param == "remote_config":
        conf = json.loads(Path(REMOTE_CONFIG_PATH).read_text(encoding="utf-8"))
        conn_conf = SurrealDBConnectionConfig(
            url=conf["url"],
            namespace=test_namespace_name,
            database=test_database_name,
            access_config=SurrealDBAccessConfig(
                username=conf.get("username", None),
                password=conf.get("password", None),
                token=conf.get("token", None),
            ),
        )

        with conn_conf.get_client() as client:
            client.query(f"DEFINE DATABASE {test_database_name};")
            client.query(f"DEFINE NAMESPACE {test_namespace_name};")
            client.use(test_namespace_name, test_database_name)
            with surrealdb_schema.open("r") as f:
                query = f.read()
            client.query(query)

        yield conn_conf

    else:
        raise ValueError(f"Unknown config type: {request.param}")


@pytest.fixture
def surrealdb_schema() -> Path:
    schema_file = assets_dir / "surrealdb-schema.sql"
    assert schema_file.exists()
    assert schema_file.is_file()
    return schema_file
