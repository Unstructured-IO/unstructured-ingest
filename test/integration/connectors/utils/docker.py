import time
from contextlib import contextmanager
from typing import Optional

import docker
from docker.models.containers import Container
from pydantic import BaseModel, Field, field_serializer


class HealthCheck(BaseModel):
    test: str
    interval: int = Field(
        gt=0, default=30, description="The time to wait between checks in seconds."
    )
    timeout: int = Field(
        gt=0, default=30, description="The time to wait before considering the check to have hung."
    )
    retries: int = Field(
        gt=0,
        default=3,
        description="The number of consecutive failures needed "
        "to consider a container as unhealthy.",
    )
    start_period: int = Field(
        gt=0,
        default=0,
        description="Start period for the container to initialize before starting health-retries countdown in seconds.",  # noqa: 501
    )

    @field_serializer("interval")
    def serialize_interval(self, interval: int) -> int:
        return int(interval * 10e8)

    @field_serializer("timeout")
    def serialize_timeout(self, timeout: int) -> int:
        return int(timeout * 10e8)

    @field_serializer("start_period")
    def serialize_start_period(self, start_period: int) -> int:
        return int(start_period * 10e8)


def get_container(
    docker_client: docker.DockerClient,
    image: str,
    ports: dict,
    environment: Optional[dict] = None,
    volumes: Optional[dict] = None,
    healthcheck: Optional[HealthCheck] = None,
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
        run_kwargs["healthcheck"] = healthcheck.model_dump()
    container: Container = docker_client.containers.run(**run_kwargs)
    return container


def has_healthcheck(container: Container) -> bool:
    return container.attrs.get("Config", {}).get("Healthcheck", None) is not None


def healthcheck_wait(container: Container, timeout: int = 30, interval: int = 1) -> None:
    health = container.health
    start = time.time()
    while health != "healthy" and time.time() - start < timeout:
        print(f"waiting for docker container to be healthy: {health}")
        time.sleep(interval)
        container.reload()
        health = container.health
    if health != "healthy":
        health_dict = container.attrs.get("State", {}).get("Health", {})
        raise TimeoutError(f"Docker container never came up healthy: {health_dict}")


@contextmanager
def container_context(
    image: str,
    ports: dict,
    environment: Optional[dict] = None,
    volumes: Optional[dict] = None,
    healthcheck: Optional[HealthCheck] = None,
    healthcheck_timeout: int = 10,
    docker_client: Optional[docker.DockerClient] = None,
):
    docker_client = docker_client or docker.from_env()
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
