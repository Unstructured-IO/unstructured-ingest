from abc import ABC
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field, SecretStr

from unstructured_ingest.utils.chunking import assign_and_map_hash_ids
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces.process import BaseProcess
from unstructured_ingest.v2.logger import logger

CHUNK_MAX_CHARS_DEFAULT: int = 500
CHUNK_MULTI_PAGE_DEFAULT: bool = True


class ChunkerConfig(BaseModel):
    chunking_strategy: Optional[str] = Field(
        default=None, description="The rule-set to use to form chunks. Omit to disable chunking."
    )
    chunking_endpoint: Optional[str] = Field(
        default="https://api.unstructured.io/general/v0/general",
        description="If chunking via api, use the following host.",
    )
    chunk_by_api: bool = Field(default=False, description="Flag to use api for chunking")
    chunk_api_key: Optional[SecretStr] = Field(
        default=None, description="API Key for chunking endpoint."
    )

    chunk_combine_text_under_n_chars: Optional[int] = Field(
        default=None,
        description="Combine consecutive chunks when the first does not exceed this length and"
        " the second will fit without exceeding the hard-maximum length. Only"
        " operative for 'by_title' chunking-strategy.",
    )
    chunk_include_orig_elements: Optional[bool] = Field(
        default=None,
        description="When chunking, add the original elements consolidated to form each chunk to"
        " `.metadata.orig_elements` on that chunk.",
    )
    chunk_max_characters: int = Field(
        default=CHUNK_MAX_CHARS_DEFAULT,
        description="Hard maximum chunk length. No chunk will exceed this length. An oversized"
        " element will be divided by text-splitting to fit this window.",
    )
    chunk_multipage_sections: bool = Field(
        default=CHUNK_MULTI_PAGE_DEFAULT,
        description="Ignore page boundaries when chunking such that elements from two different"
        " pages can appear in the same chunk. Only operative for 'by_title'"
        " chunking-strategy.",
    )
    chunk_new_after_n_chars: Optional[int] = Field(
        default=None,
        description="Soft-maximum chunk length. Another element will not be added to a chunk of"
        " this length even when it would fit without exceeding the hard-maximum"
        " length.",
    )
    chunk_overlap: Optional[int] = Field(
        default=None,
        description="Prefix chunk text with last overlap=N characters of prior chunk. Only"
        " applies to oversized chunks divided by text-splitting. To apply overlap to"
        " non-oversized chunks use the --overlap-all option.",
    )
    chunk_overlap_all: Optional[bool] = Field(
        default=None,
        description="Apply overlap to chunks formed from whole elements as well as those formed"
        " by text-splitting oversized elements. Overlap length is take from --overlap"
        " option value.",
    )

    def to_chunking_kwargs(self) -> dict[str, Any]:
        return {
            "chunking_strategy": self.chunking_strategy,
            "combine_under_n_chars": self.chunk_combine_text_under_n_chars,
            "max_characters": self.chunk_max_characters,
            "include_orig_elements": self.chunk_include_orig_elements,
            "multipage_sections": self.chunk_multipage_sections,
            "new_after_n_chars": self.chunk_new_after_n_chars,
            "overlap": self.chunk_overlap,
            "overlap_all": self.chunk_overlap_all,
        }


@dataclass
class Chunker(BaseProcess, ABC):
    config: ChunkerConfig

    def is_async(self) -> bool:
        return self.config.chunk_by_api

    @requires_dependencies(dependencies=["unstructured"])
    def run(self, elements_filepath: Path, **kwargs: Any) -> list[dict]:
        from unstructured.chunking import dispatch
        from unstructured.staging.base import elements_from_json

        elements = elements_from_json(filename=str(elements_filepath))
        if not elements:
            return [e.to_dict() for e in elements]
        local_chunking_strategies = ("basic", "by_title")
        if self.config.chunking_strategy not in local_chunking_strategies:
            logger.warning(
                "chunking strategy not supported for local chunking: {}, must be one of: {}".format(
                    self.config.chunking_strategy, ", ".join(local_chunking_strategies)
                )
            )
            return [e.to_dict() for e in elements]
        chunked_elements = dispatch.chunk(elements=elements, **self.config.to_chunking_kwargs())
        chunked_elements_dicts = [e.to_dict() for e in chunked_elements]
        chunked_elements_dicts = assign_and_map_hash_ids(elements=chunked_elements_dicts)
        return chunked_elements_dicts

    @requires_dependencies(dependencies=["unstructured_client"], extras="remote")
    async def run_async(self, elements_filepath: Path, **kwargs: Any) -> list[dict]:
        from unstructured_client import UnstructuredClient
        from unstructured_client.models.operations import PartitionRequest
        from unstructured_client.models.shared import Files, PartitionParameters

        client = UnstructuredClient(
            api_key_auth=self.config.chunk_api_key.get_secret_value(),
            server_url=self.config.chunking_endpoint,
        )
        partition_request = self.config.to_chunking_kwargs()
        possible_fields = [f.name for f in fields(PartitionParameters)]
        filtered_partition_request = {
            k: v for k, v in partition_request.items() if k in possible_fields
        }
        if len(filtered_partition_request) != len(partition_request):
            logger.debug(
                "Following fields were omitted due to not being "
                "supported by the currently used unstructured client: {}".format(
                    ", ".join([v for v in partition_request if v not in filtered_partition_request])
                )
            )
        with open(elements_filepath, "rb") as f:
            files = Files(
                content=f.read(),
                file_name=str(elements_filepath.resolve()),
            )
            filtered_partition_request["files"] = files
            partition_params = PartitionParameters(**filtered_partition_request)
            partition_request_obj = PartitionRequest(partition_params)
        resp = client.general.partition(partition_request_obj)
        elements = resp.elements or []
        elements = assign_and_map_hash_ids(elements=elements)
        return elements
