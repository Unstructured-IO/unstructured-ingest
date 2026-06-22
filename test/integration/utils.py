import asyncio
import os
from collections.abc import Awaitable, Callable
from typing import TypeVar

import pytest

from unstructured_ingest.error import ProviderError, RateLimitError
from unstructured_ingest.error import TimeoutError as IngestTimeoutError
from unstructured_ingest.logger import logger

T = TypeVar("T")

# Failures that are transient when talking to a live service (the hosted partition API in
# particular) and are safe to retry: 5xx responses (ProviderError) and request timeouts
# (IngestTimeoutError). Raw transport-level drops from the underlying http stack
# (httpx/httpcore ReadError, ConnectError, timeouts) are matched separately by module name in
# ``_is_transient`` so this module doesn't take a hard dependency on the http stack just to
# enumerate exception types. RateLimitError is listed for completeness as a generically-retryable
# class, though note the current API wrapper (unstructured_api.wrap_error) maps 429 to UserError,
# so a throttle from the hosted partitioner today surfaces as a non-transient UserError and is
# NOT retried — retrying generic UserError is deliberately avoided so real input errors fail fast.
_TRANSIENT_ERROR_TYPES: tuple[type[BaseException], ...] = (
    ProviderError,
    RateLimitError,
    IngestTimeoutError,
    asyncio.TimeoutError,
)


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, _TRANSIENT_ERROR_TYPES):
        return True
    # httpx / httpcore transport errors (ReadError, ConnectError, ReadTimeout, ...) can surface
    # un-wrapped from the SDK. They are only importable when the http stack is installed, so match
    # by originating module to avoid importing them here.
    module = type(exc).__module__
    return module.startswith("httpx") or module.startswith("httpcore")


async def retry_async(
    factory: Callable[[], Awaitable[T]],
    *,
    attempts: int = 3,
    per_attempt_timeout_s: float | None = None,
    backoff_s: float = 2.0,
) -> T:
    """Call an async ``factory``, retrying only on transient live-service failures.

    ``factory`` must return a *fresh* awaitable on each call so every attempt issues a new
    request. Non-transient exceptions (auth errors, input/validation errors, bad output) propagate
    immediately on the first attempt — this only smooths over flaky infrastructure, it never
    masks a real failure. ``per_attempt_timeout_s`` wraps each attempt in ``asyncio.wait_for`` so a
    hung call is cancelled and retried instead of consuming the whole test-level timeout budget.
    """
    last_exc: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            awaitable = factory()
            if per_attempt_timeout_s is not None:
                return await asyncio.wait_for(awaitable, timeout=per_attempt_timeout_s)
            return await awaitable
        except Exception as exc:
            if not _is_transient(exc) or attempt == attempts:
                raise
            last_exc = exc
            logger.warning(
                f"transient failure on attempt {attempt}/{attempts} "
                f"({type(exc).__name__}: {exc}); retrying in {backoff_s}s"
            )
            await asyncio.sleep(backoff_s)
    # Unreachable: the loop either returns or re-raises on the final attempt.
    raise last_exc  # type: ignore[misc]


def requires_env(*envs):
    if len(envs) == 1:
        env = envs[0]
        return pytest.mark.skipif(
            env not in os.environ, reason=f"Environment variable not set: {env}"
        )
    return pytest.mark.skipif(
        not all(env in os.environ for env in envs),
        reason="All required environment variables not set: {}".format(", ".join(envs)),
    )
