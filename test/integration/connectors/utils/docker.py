import time
from contextlib import contextmanager
from typing import Optional

import docker
from docker.models.containers import Container


def get_container(
    docker_client: docker.DockerClient,
    image: str,
    ports: dict,
    environment: Optional[dict] = None,
    volumes: Optional[dict] = None,
    healthcheck: Optional[dict] = None,
) -> Container:
    run_kwargs = {
        "image": image,
        "detach": True,
        "ports": ports,
    }
    if environment:
        run_kwargs["environment"] = environment
    if volumes:
        run_kwargs["volumes"] = volumes
    if healthcheck:
        run_kwargs["healthcheck"] = healthcheck
    container: Container = docker_client.containers.run(**run_kwargs)
    return container


def has_healthcheck(container: Container) -> bool:
    return container.attrs.get("Config", {}).get("Healthcheck", None) is not None


def healthcheck_wait(container: Container, timeout: int = 10) -> None:
    health = container.health
    start = time.time()
    while health != "healthy" and time.time() - start < timeout:
        time.sleep(1)
        container.reload()
        health = container.health
    if health != "healthy":
        health_dict = container.attrs.get("State", {}).get("Health", {})
        raise TimeoutError(f"Docker container never came up healthy: {health_dict}")


@contextmanager
def container_context(
    docker_client: docker.DockerClient,
    image: str,
    ports: dict,
    environment: Optional[dict] = None,
    volumes: Optional[dict] = None,
    healthcheck: Optional[dict] = None,
    healthcheck_timeout: int = 10,
):
    container: Optional[Container] = None
    try:
        container = get_container(
            docker_client=docker_client,
            image=image,
            ports=ports,
            environment=environment,
            volumes=volumes,
            healthcheck=healthcheck,
        )
        if has_healthcheck(container):
            healthcheck_wait(container=container, timeout=healthcheck_timeout)
        yield container
    except AssertionError as e:
        if container:
            logs = container.logs()
            print(logs.decode("utf-8"))
            raise e
    finally:
        if container:
            container.kill()
