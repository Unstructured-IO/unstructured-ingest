from dataclasses import fields
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from unstructured_ingest.v2.errors import ProviderError, QuotaError, UserAuthError, UserError
from unstructured_ingest.v2.logger import logger

if TYPE_CHECKING:
    from unstructured_client.models.operations import PartitionRequest


def create_partition_request(filename: Path, parameters_dict: dict) -> "PartitionRequest":
    """Given a filename and a dict of API parameters, return a PartitionRequest for use
    by unstructured-client. Remove any params that aren't recognized by the SDK.

    Args:
        filename: Path to the file being partitioned
        parameters_dict: A mapping of all API params we want to send

    Returns: A PartitionRequest containing the file and all valid params
    """
    from unstructured_client.models.operations import PartitionRequest
    from unstructured_client.models.shared import Files, PartitionParameters

    # NOTE(austin): PartitionParameters is a Pydantic model in v0.26.0
    # Prior to this it was a dataclass which doesn't have .__fields
    try:
        possible_fields = PartitionParameters.model_fields
    except AttributeError:
        possible_fields = [f.name for f in fields(PartitionParameters)]

    filtered_partition_request = {k: v for k, v in parameters_dict.items() if k in possible_fields}
    if len(filtered_partition_request) != len(parameters_dict):
        logger.debug(
            "Following fields were omitted due to not being "
            "supported by the currently used unstructured client: {}".format(
                ", ".join([v for v in parameters_dict if v not in filtered_partition_request])
            )
        )

    logger.debug(f"using hosted partitioner with kwargs: {parameters_dict}")

    with open(filename, "rb") as f:
        files = Files(
            content=f.read(),
            file_name=str(filename.resolve()),
        )
        filtered_partition_request["files"] = files

    partition_params = PartitionParameters(**filtered_partition_request)

    return PartitionRequest(partition_parameters=partition_params)


def wrap_error(e: Exception) -> Exception:
    from unstructured_client.models.errors.httpvalidationerror import HTTPValidationError
    from unstructured_client.models.errors.sdkerror import SDKError
    from unstructured_client.models.errors.servererror import ServerError

    if isinstance(e, HTTPValidationError):
        return UserError(e.data.detail)
    if isinstance(e, ServerError):
        return ProviderError(e.data.detail)

    if not isinstance(e, SDKError):
        logger.error(f"Uncaught Error calling API: {e}")
        raise e
    status_code = e.status_code
    body = e.body
    if status_code == 402:
        return QuotaError(body)
    if status_code in [401, 403]:
        return UserAuthError(body)
    if 400 <= status_code < 500:
        return UserError(body)
    if status_code >= 500:
        return ProviderError(body)
    logger.error(f"Uncaught Error calling API: {e}")
    raise e


async def call_api_async(
    server_url: Optional[str], api_key: Optional[str], filename: Path, api_parameters: dict
) -> list[dict]:
    """Call the Unstructured API using unstructured-client.

    Args:
        server_url: The base URL where the API is hosted
        api_key: The user's API key (can be empty if this is a self hosted API)
        filename: Path to the file being partitioned
        api_parameters: A dict containing the requested API parameters

    Returns: A list of the file's elements, or an empty list if there was an error
    """
    from unstructured_client import UnstructuredClient

    client = UnstructuredClient(
        server_url=server_url,
        api_key_auth=api_key,
    )
    partition_request = create_partition_request(filename=filename, parameters_dict=api_parameters)
    try:
        res = await client.general.partition_async(request=partition_request)
    except Exception as e:
        raise wrap_error(e)

    return res.elements or []


def call_api(
    server_url: Optional[str], api_key: Optional[str], filename: Path, api_parameters: dict
) -> list[dict]:
    """Call the Unstructured API using unstructured-client.

    Args:
        server_url: The base URL where the API is hosted
        api_key: The user's API key (can be empty if this is a self hosted API)
        filename: Path to the file being partitioned
        api_parameters: A dict containing the requested API parameters

    Returns: A list of the file's elements, or an empty list if there was an error
    """
    from unstructured_client import UnstructuredClient

    client = UnstructuredClient(
        server_url=server_url,
        api_key_auth=api_key,
    )
    partition_request = create_partition_request(filename=filename, parameters_dict=api_parameters)
    try:
        res = client.general.partition(request=partition_request)
    except Exception as e:
        raise wrap_error(e)

    return res.elements or []
