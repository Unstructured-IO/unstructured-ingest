import subprocess
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def docker_compose_context(docker_compose_path: Path):
    # Dynamically run a specific docker compose file and make sure it gets cleanup by
    # by leveraging a context manager. Uses subprocess to map docker compose commands
    # to the underlying shell.
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
