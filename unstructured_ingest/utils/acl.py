"""Helpers for deriving a stable ACL digest from a record's permissions.

PLU-511: the digest is compared alongside the content version token so that an
ACL-only change at the source (content unchanged) still triggers a reprocess
under incremental. It must be deterministic: the same permissions in a different
order must produce the same digest, or reordering causes false-positive
reprocesses (see acceptance-test scenario S5).
"""

import hashlib
import json
from typing import Any, Optional


def _canonicalize(obj: Any) -> Any:
    """Recursively sort dict keys and lists so equivalent permissions hash equally."""
    if isinstance(obj, dict):
        return {key: _canonicalize(obj[key]) for key in sorted(obj)}
    if isinstance(obj, list):
        canon_items = [_canonicalize(item) for item in obj]
        return sorted(canon_items, key=lambda item: json.dumps(item, sort_keys=True))
    return obj


def compute_permissions_version(
    permissions_data: Optional[list[dict[str, Any]]],
    denied_permissions_data: Optional[list[dict[str, Any]]] = None,
) -> Optional[str]:
    """Return a stable SHA-256 digest of a record's ACL, or None if there is none.

    Returns None only when there is no ACL signal at all (the connector did not
    emit permissions). An empty list is a real state (e.g. all access revoked)
    and produces a stable, non-None digest so revocation is detected.
    """
    if permissions_data is None and denied_permissions_data is None:
        return None
    payload = {
        "permissions": _canonicalize(permissions_data or []),
        "denied": _canonicalize(denied_permissions_data or []),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
