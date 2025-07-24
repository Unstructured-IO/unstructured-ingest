import os
import ssl

import certifi


def ssl_context_with_optional_ca_override():
    """
    # https://www.python-httpx.org/advanced/ssl/#working-with-ssl_cert_file-and-ssl_cert_dir
    # We choose REQUESTS_CA_BUNDLE because that works with many other Python packages.
    """
    return ssl.create_default_context(
        cafile=os.environ.get("REQUESTS_CA_BUNDLE", certifi.where()),
        capath=os.environ.get("REQUESTS_CA_BUNDLE"),
    )
