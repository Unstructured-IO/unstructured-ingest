"""PLU-511: ACL digest determinism and behavior (acceptance-test scenario S5)."""

from unstructured_ingest.utils.acl import compute_permissions_version


def _perms(read_users):
    return [
        {"read": {"users": read_users, "groups": []}},
        {"update": {"users": [], "groups": []}},
        {"delete": {"users": [], "groups": []}},
    ]


def test_none_permissions_returns_none():
    # No ACL signal at all -> no digest (connector did not emit permissions).
    assert compute_permissions_version(None) is None


def test_empty_permissions_is_a_real_digest():
    # Empty list is a real state (e.g. all access revoked), not "no signal".
    digest = compute_permissions_version([])
    assert digest is not None
    assert compute_permissions_version(None) != digest


def test_same_permissions_same_digest():
    assert compute_permissions_version(_perms(["a", "b"])) == compute_permissions_version(
        _perms(["a", "b"])
    )


def test_reordered_users_same_digest():
    # S5: same permissions, different order -> identical digest, no false positive.
    assert compute_permissions_version(_perms(["a", "b"])) == compute_permissions_version(
        _perms(["b", "a"])
    )


def test_reordered_operations_same_digest():
    # The list of operation dicts may come back in any order.
    forward = [
        {"read": {"users": ["a"], "groups": []}},
        {"update": {"users": [], "groups": []}},
    ]
    reversed_order = [
        {"update": {"users": [], "groups": []}},
        {"read": {"users": ["a"], "groups": []}},
    ]
    assert compute_permissions_version(forward) == compute_permissions_version(reversed_order)


def test_real_change_changes_digest():
    # S2/S9: a genuine ACL change must move the digest.
    assert compute_permissions_version(_perms(["a"])) != compute_permissions_version(
        _perms(["a", "c"])
    )


def test_revocation_changes_digest():
    # S9: going from granted to fully revoked (empty) must change the digest.
    assert compute_permissions_version(_perms(["a", "b"])) != compute_permissions_version([])


def test_denied_permissions_affect_digest():
    # Deny-side changes (FileNet sibling keys) must also move the digest.
    base = _perms(["a"])
    assert compute_permissions_version(base, denied_permissions_data=None) != (
        compute_permissions_version(base, denied_permissions_data=[{"read": {"deny_users": ["x"]}}])
    )
