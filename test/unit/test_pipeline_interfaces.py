from dataclasses import dataclass
from typing import Any, Callable

import pytest

from unstructured_ingest.interfaces import BaseProcess, ProcessorConfig
from unstructured_ingest.pipeline.interfaces import PipelineStep


@dataclass
class AsyncEchoProcess(BaseProcess):
    def is_async(self) -> bool:
        return True

    def run(self, **kwargs: Any) -> Any:
        return kwargs["value"]

    async def run_async(self, **kwargs: Any) -> Any:
        return f"async:{kwargs['value']}"


@dataclass
class AsyncEchoStep(PipelineStep):
    identifier: str = "async-echo"

    async def _run_async(self, fn: Callable, **kwargs: Any) -> Any:
        return await fn(**kwargs)


def test_process_async_without_running_loop():
    step = AsyncEchoStep(process=AsyncEchoProcess(), context=ProcessorConfig(max_connections=1))

    result = step.process_async(iterable=[{"value": "hello"}])

    assert result == ["async:hello"]


@pytest.mark.asyncio
async def test_process_async_with_running_loop():
    step = AsyncEchoStep(process=AsyncEchoProcess(), context=ProcessorConfig(max_connections=1))

    result = step.process_async(iterable=[{"value": "hello"}])

    assert result == ["async:hello"]
