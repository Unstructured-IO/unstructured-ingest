from __future__ import annotations

import asyncio
import importlib
from functools import wraps
from typing import (
    Callable,
    List,
    Optional,
    TypeVar,
)

from typing_extensions import ParamSpec

_T = TypeVar("_T")
_P = ParamSpec("_P")


def requires_dependencies(
    dependencies: str | list[str],
    extras: Optional[str] = None,
) -> Callable[[Callable[_P, _T]], Callable[_P, _T]]:
    if isinstance(dependencies, str):
        dependencies = [dependencies]

    def decorator(func: Callable[_P, _T]) -> Callable[_P, _T]:
        def run_check():
            missing_deps: List[str] = []
            for dep in dependencies:
                if not dependency_exists(dep):
                    missing_deps.append(dep)
            if len(missing_deps) > 0:
                raise ImportError(
                    f"Following dependencies are missing: {', '.join(missing_deps)}. "
                    + (
                        f"""Please install them using `pip install "unstructured-ingest[{extras}]"`."""  # noqa: E501
                        if extras
                        else f"Please install them using `pip install {' '.join(missing_deps)}`."
                    ),
                )

        @wraps(func)
        def wrapper(*args: _P.args, **kwargs: _P.kwargs):
            run_check()
            return func(*args, **kwargs)

        @wraps(func)
        async def wrapper_async(*args: _P.args, **kwargs: _P.kwargs):
            run_check()
            return await func(*args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            return wrapper_async
        return wrapper

    return decorator


def dependency_exists(dependency: str):
    try:
        importlib.import_module(dependency)
    except ImportError as e:
        # Check to make sure this isn't some unrelated import error.
        if dependency in repr(e):
            return False
    return True
