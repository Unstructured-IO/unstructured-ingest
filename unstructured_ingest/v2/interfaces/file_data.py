"""
COMPATABILITY NOTICE:
This file has moved to the v2/types/ module.
The following line exists for backward compatibility.
"""
from unstructured_ingest.v2.types.file_data import *
logger = logging.getLogger(__name__)
logger.warning("Importing file_data.py through interfaces is deprecated. "
               "Please use unstructured_ingest.v2.types.file_data instead!")