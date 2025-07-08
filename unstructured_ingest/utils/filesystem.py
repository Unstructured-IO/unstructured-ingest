"""
Filesystem utilities for concurrent operations.

This module provides race-condition-safe filesystem operations that are needed
when multiple processes operate on the same directory structures simultaneously.
"""

from pathlib import Path


def ensure_directory(path: Path) -> None:
    """
    Ensure directory exists, handling race conditions in concurrent environments.
    
    This addresses the issue where Path.mkdir(parents=True, exist_ok=True) can still
    raise FileExistsError when multiple processes attempt to create overlapping 
    directory structures simultaneously.
    
    Related: https://github.com/python/cpython/pull/112966/files
    Python core team used the same approach to fix zipfile race conditions.
    """
    try:
        path.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        if not (path.exists() and path.is_dir()):
            raise