"""
COMPATABILITY NOTICE:
This file has moved to the v2/types/ module.
The following line exists for backward compatibility.
"""

from unstructured_ingest.v2.types.file_data import *  # noqa - star imports are bad, but this is for maximal backward compatability

#  Eventually this file should go away. Let's start warning users now:
logger.warning(  # noqa - using logger from the star import
    "Importing file_data.py through interfaces is deprecated. "
    "Please use unstructured_ingest.v2.types.file_data instead!"
)
