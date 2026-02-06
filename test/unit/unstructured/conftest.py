import importlib.util

# Prevent collection of tests in this directory when the unstructured
# package is not installed. These tests import unstructured at the module
# level, causing ImportError during collection in environments that
# don't have it (e.g. integration test CI jobs).
if importlib.util.find_spec("unstructured") is None:
    collect_ignore_glob = ["*.py"]
