"""
Filesystem utilities for concurrent operations.

This module provides race-condition-safe filesystem operations that are needed
when multiple processes operate on the same directory structures simultaneously.
"""

import hashlib
from pathlib import Path
from typing import Optional


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


def generate_hash_based_path(s3_key: str, base_dir: Optional[Path] = None) -> Optional[Path]:
    """
    Generate hash-based download path to prevent S3 path conflicts.
    
    Prevents conflicts when S3's flat namespace is mapped to hierarchical filesystems.
    E.g., 'foo' (file) and 'foo/document' (needs foo as directory) would conflict.
    """
    if not s3_key:
        return None
        
    normalized_path = s3_key.lstrip("/")
    if not normalized_path or not normalized_path.strip():
        return None
    
    filename = Path(normalized_path).name
    if not filename:
        return None
    
    dir_hash = hashlib.sha256(normalized_path.encode('utf-8')).hexdigest()[:12]
    relative_path = Path(dir_hash) / filename
    return base_dir / relative_path if base_dir else relative_path