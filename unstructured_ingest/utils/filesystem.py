"""
Filesystem utilities for concurrent operations.

This module provides race-condition-safe filesystem operations that are needed
when multiple processes operate on the same directory structures simultaneously.
"""

from pathlib import Path


def mkdir_concurrent_safe(path: Path) -> None:
    """
    Create directory safely in concurrent environments, handling race conditions.
    
    This addresses the issue where Path.mkdir(parents=True, exist_ok=True) can still
    raise FileExistsError when multiple processes attempt to create overlapping 
    directory structures simultaneously. In this codebase, this occurs when multiple
    files are being downloaded in parallel and archive extraction is happening in parallel.
    
    Related: https://github.com/python/cpython/pull/112966/files
    Python core team used the same approach to fix zipfile race conditions.
    """
    try:
        path.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        if not (path.exists() and path.is_dir()):
            raise