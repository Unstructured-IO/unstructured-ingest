import time
from contextlib import contextmanager
from typing import Optional, Union

import docker
from docker.models.containers import Container
from pydantic import BaseModel, Field, field_serializer


class HealthCheck(BaseModel):
    test: Union[str, list[str]]
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
        description="Start period for the container to initialize before starting health-retries countdown in seconds.",  # noqa: E501
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
    name: Optional[str] = None,
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
    if name:
        run_kwargs["name"] = name
    container: Container = docker_client.containers.run(**run_kwargs)
    return container


def get_healthcheck(container: Container) -> Optional[HealthCheck]:
    healthcheck_config = container.attrs.get("Config", {}).get("Healthcheck", None)
    if not healthcheck_config:
        return None
    healthcheck_data = {
        "test": healthcheck_config["Test"],
    }
    if interval := healthcheck_config.get("Interval"):
        healthcheck_data["interval"] = interval / 10e8
    if start_period := healthcheck_config.get("StartPeriod"):
        healthcheck_data["start_period"] = start_period / 10e8
    if retries := healthcheck_config.get("Retries"):
        healthcheck_data["retries"] = retries
    return HealthCheck.model_validate(healthcheck_data)


def healthcheck_wait(
    container: Container, retries: int = 30, interval: int = 1, start_period: Optional[int] = None
) -> None:
    if start_period:
        time.sleep(start_period)
    health = container.health
    tries = 0
    while health != "healthy" and tries < retries:
        tries += 1
        logs = container.attrs.get("State", {}).get("Health", {}).get("Log")
        latest_log = logs[-1] if logs else None
        print(
            f"attempt {tries} - waiting for docker container "
            f"to be healthy: {health} latest log: {latest_log}"
        )
        time.sleep(interval)
        container.reload()
        health = container.health
    if health != "healthy":
        logs = container.attrs.get("State", {}).get("Health", {}).get("Log")
        latest_log = logs[-1] if logs else None
        raise TimeoutError(f"Docker container never came up healthy: {latest_log}")


@contextmanager
def container_context(
    image: str,
    ports: dict,
    environment: Optional[dict] = None,
    volumes: Optional[dict] = None,
    healthcheck: Optional[HealthCheck] = None,
    healthcheck_retries: int = 30,
    docker_client: Optional[docker.DockerClient] = None,
    name: Optional[str] = None,
):
    docker_client = docker_client or docker.from_env()
    print(f"pulling image {image}")
    docker_client.images.pull(image)
    container: Optional[Container] = None
    try:
        container = get_container(
            docker_client=docker_client,
            image=image,
            ports=ports,
            environment=environment,
            volumes=volumes,
            healthcheck=healthcheck,
            name=name,
        )
        if healthcheck_data := get_healthcheck(container):
            # Mirror whatever healthcheck config set on container
            healthcheck_wait(
                container=container,
                retries=healthcheck_retries,
                start_period=healthcheck_data.start_period,
                interval=healthcheck_data.interval,
            )
        yield container
    except AssertionError as e:
        if container:
            logs = container.logs()
            print(logs.decode("utf-8"))
            raise e
    finally:
        if container:
            container.kill()
            container.remove()
