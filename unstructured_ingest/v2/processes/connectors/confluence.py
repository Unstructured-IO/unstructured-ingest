import math
from dataclasses import dataclass, field

from unstructured_ingest.enhanced_dataclass import enhanced_field
from unstructured_ingest.interfaces import (
    AccessConfig,
    BaseConnectorConfig,
)


@dataclass
class ConfluenceAccessConfig(AccessConfig):
    api_token: str = enhanced_field(sensitive=True)  # remove enhanced


@dataclass
class ConfluenceConnectorConfig(BaseConnectorConfig):
    user_email: str
    access_config: ConfluenceAccessConfig
    url: str
    max_num_of_spaces: int = 500  # 500?
    max_num_of_docs_per_space: int = 100  # 100?
    spaces: list[str] = field(default_factory=list)


@dataclass
class ConfluenceDocumentMeta:
    space_id: str
    document_id: str


def paginate_results(func):
    def wrapper(*args, **kwargs):
        total_items = kwargs.pop("number_of_items_to_fetch")
        kwargs["limit"] = min(100, total_items)
        kwargs["start"] = kwargs.get("start", 0)

        all_results = []
        num_iterations = math.ceil(total_items / kwargs["limit"])

        for _ in range(num_iterations):
            response = func(*args, **kwargs)
            if isinstance(response, list):
                all_results.extend(response)
            elif isinstance(response, dict):
                all_results.extend(response.get("results", []))
            kwargs["start"] += kwargs["limit"]
