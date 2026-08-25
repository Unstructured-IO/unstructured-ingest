"""Unit tests for DataSanitizer.

Focus: URL sanitization must drop userinfo. fsspec-style remote URLs
("s3://user:password@bucket/path") embed credentials in the netloc, and
sanitize_url feeds connector error logs via log_connection_failed.
"""

import pytest

from unstructured_ingest.processes.utils.logging.sanitizer import DataSanitizer


class TestSanitizeUrl:
    @pytest.mark.parametrize(
        ("url", "expected"),
        [
            # Credentials in the netloc must never survive.
            ("s3://myuser:s3cr3t@bucket/path", "s3://***@bucket/path"),
            ("sftp://user:pw@host.example.com:2222/data", "sftp://***@host.example.com:2222/data"),
            # A secret can occupy the username slot alone (token-style URLs).
            ("https://ghp_tokenvalue@github.com/o/r", "https://***@github.com/o/r"),
            # Empty password still counts as userinfo.
            ("s3://myuser:@bucket/path", "s3://***@bucket/path"),
        ],
    )
    def test_userinfo_is_removed(self, url, expected):
        assert DataSanitizer.sanitize_url(url) == expected

    @pytest.mark.parametrize(
        ("url", "expected"),
        [
            ("sftp://host.example.com:2222/data", "sftp://host.example.com:2222/data"),
            ("sftp:///data", "sftp:///data"),
            # Host casing and IPv6 brackets are preserved (we split the raw netloc
            # rather than rebuilding from parsed.hostname, which lowercases).
            ("s3://[2001:db8::1]:9000/b", "s3://[2001:db8::1]:9000/b"),
            ("https://API.Example.COM/p", "https://API.Example.COM/p"),
        ],
    )
    def test_credential_free_urls_are_unchanged(self, url, expected):
        assert DataSanitizer.sanitize_url(url) == expected

    def test_query_parameters_are_dropped(self):
        assert DataSanitizer.sanitize_url("https://host/p?sig=secret&x=1") == "https://host/p"

    def test_empty_url(self):
        assert DataSanitizer.sanitize_url("") == "<url>"

    def test_secret_never_appears_in_output(self):
        secret = "SUPERSECRETPW"
        out = DataSanitizer.sanitize_url(f"s3://user:{secret}@bucket/key?sig={secret}")
        assert secret not in out
