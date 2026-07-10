"""Fetch Transform MCP output (Element JSON) for local ingestion.

The agent hands this server a ``download_url`` from ``get_transform_results`` (or
a local path it already saved). The server pulls the bytes itself so the —
potentially large — JSON never passes through the agent's context, which is the
whole point of doing the load out of band. The download is streamed with a byte
cap so a runaway or wrong URL can't exhaust memory.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

# Generous enough for a large multi-thousand-page render on a slow link, tight
# enough that an unreachable host fails rather than hanging the tool call.
_FETCH_TIMEOUT_S = 120.0

# Spill to disk past this while streaming, so a large body is bounded in RSS.
_SPOOL_THRESHOLD_BYTES = 8 * 1024 * 1024


def fetch_elements(source: str, *, max_bytes: int) -> list[dict[str, Any]]:
    """Return the Element JSON array at ``source`` (http(s) URL or local path)."""
    parsed = urlparse(source)
    scheme = parsed.scheme.lower()
    if scheme in ("http", "https"):
        elements = _stream_json(source, max_bytes=max_bytes)
    elif scheme in ("", "file"):
        path = Path(parsed.path if scheme == "file" else source)
        raw = path.read_bytes()
        if len(raw) > max_bytes:
            raise ValueError(f"{source}: {len(raw)} bytes exceeds cap of {max_bytes}")
        elements = json.loads(raw)
    else:
        raise ValueError(f"unsupported source scheme {scheme!r}: {source}")

    if not isinstance(elements, list):
        raise ValueError(
            f"{source}: expected a JSON array of elements, got {type(elements).__name__}. "
            "Fetch get_transform_results with output_format='json'."
        )
    return elements


def _stream_json(url: str, *, max_bytes: int) -> Any:
    total = 0
    # SpooledTemporaryFile keeps a small body in memory and spills a large one to
    # disk; either way the parse reads from a single seekable handle.
    with tempfile.SpooledTemporaryFile(max_size=_SPOOL_THRESHOLD_BYTES, mode="w+b") as spool:
        with httpx.stream("GET", url, timeout=_FETCH_TIMEOUT_S, follow_redirects=True) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_bytes():
                total += len(chunk)
                if total > max_bytes:
                    raise ValueError(f"{url}: download exceeds cap of {max_bytes} bytes")
                spool.write(chunk)
        spool.seek(0)
        return json.load(spool)
