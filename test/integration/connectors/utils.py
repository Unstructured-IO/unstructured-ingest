import subprocess
from contextlib import contextmanager
from pathlib import Path

SOURCE_TAG = "source"
DESTINATION_TAG = "destination"

env_setup_path = Path(__file__).parents[3] / "test_e2e" / "env_setup"


@contextmanager
def docker_compose_context(docker_compose_path: Path):
    assert docker_compose_path.exists()
    if docker_compose_path.is_dir():
        if (docker_compose_path / "docker-compose.yml").exists():
            docker_compose_path = docker_compose_path / "docker-compose.yml"
        elif (docker_compose_path / "docker-compose.yaml").exists():
            docker_compose_path = docker_compose_path / "docker-compose.yaml"
    assert docker_compose_path.is_file()
    try:
        cmd = f"docker compose -f {docker_compose_path.resolve()} up -d --wait"
        print(f"Running command: {cmd}")
        resp = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
        )
        # Return code from docker compose using --wait can be 1 even if no error
        print("STDOUT: {}".format(resp.stdout.decode("utf-8")))
        print("STDERR: {}".format(resp.stderr.decode("utf-8")))
        yield
    finally:
        cmd = f"docker compose -f {docker_compose_path.resolve()} down --remove-orphans -v"
        print(f"Running command: {cmd}")
        final_resp = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
        )
        print("STDOUT: {}".format(final_resp.stdout.decode("utf-8")))
        print("STDERR: {}".format(final_resp.stderr.decode("utf-8")))
