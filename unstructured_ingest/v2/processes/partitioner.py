from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field, SecretStr

from unstructured_ingest.utils.data_prep import flatten_dict
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces.process import BaseProcess
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.unstructured_api import call_api_async


class PartitionerConfig(BaseModel):
    strategy: str = Field(
        default="auto",
        description="The method that will be used to process the documents. ",
        examples=["fast", "hi_res", "auto"],
    )
    ocr_languages: Optional[list[str]] = Field(
        default=None,
        description="A list of language packs to specify which languages to use for OCR, "
        "The appropriate Tesseract language pack needs to be installed.",
        examples=["eng", "deu", "eng,deu"],
    )
    encoding: Optional[str] = Field(
        default=None,
        description="Text encoding to use when reading documents. "
        "By default the encoding is detected automatically.",
    )
    additional_partition_args: Optional[dict[str, Any]] = Field(
        default=None, description="Additional values to pass through to partition()"
    )
    skip_infer_table_types: Optional[list[str]] = Field(
        default=None, description="Optional list of document types to skip table extraction on"
    )
    fields_include: list[str] = Field(
        default_factory=lambda: ["element_id", "text", "type", "metadata", "embeddings"],
        description="If set, include the specified top-level fields in an element.",
    )
    flatten_metadata: bool = Field(
        default=False,
        description="Results in flattened json elements. "
        "Specifically, the metadata key values are brought to "
        "the top-level of the element, and the `metadata` key itself is removed.",
    )
    metadata_exclude: list[str] = Field(
        default_factory=list,
        description="If set, drop the specified metadata fields if they exist.",
    )
    element_exclude: list[str] = Field(
        default_factory=list,
        description="If set, drop the specified element types, if they exist.",
    )
    metadata_include: list[str] = Field(
        default_factory=list,
        description="If set, include the specified metadata "
        "fields if they exist and drop all other fields. ",
    )
    partition_endpoint: Optional[str] = Field(
        default="https://api.unstructuredapp.io/general/v0/general",
        description="If partitioning via api, use the following host.",
    )
    partition_by_api: bool = Field(
        default=False,
        description="Use a remote API to partition the files."
        " Otherwise, use the function from partition.auto",
    )
    api_key: Optional[SecretStr] = Field(
        default=None, description="API Key for partition endpoint."
    )
    hi_res_model_name: Optional[str] = Field(
        default=None, description="Model name for hi-res strategy."
    )

    def model_post_init(self, __context: Any) -> None:
        if self.metadata_exclude and self.metadata_include:
            raise ValueError(
                "metadata_exclude and metadata_include are "
                "mutually exclusive with each other. Cannot specify both."
            )

    def to_partition_kwargs(self) -> dict[str, Any]:
        partition_kwargs: dict[str, Any] = {
            "strategy": self.strategy,
            "languages": self.ocr_languages,
            "hi_res_model_name": self.hi_res_model_name,
            "skip_infer_table_types": self.skip_infer_table_types,
        }
        # Don't inject information if None and allow default values in method to be used
        partition_kwargs = {k: v for k, v in partition_kwargs.items() if v is not None}
        if self.additional_partition_args:
            partition_kwargs.update(self.additional_partition_args)
        return partition_kwargs


@dataclass
class Partitioner(BaseProcess, ABC):
    config: PartitionerConfig

    def is_async(self) -> bool:
        return self.config.partition_by_api

    def postprocess(self, elements: list[dict]) -> list[dict]:
        element_dicts = [e.copy() for e in elements]
        if self.config.element_exclude:
            element_dicts = list(
                filter(
                    lambda element: element["type"] not in self.config.element_exclude,
                    element_dicts,
                )
            )
        for elem in element_dicts:
            if self.config.metadata_exclude:
                ex_list = self.config.metadata_exclude
                for ex in ex_list:
                    if "." in ex:  # handle nested fields
                        nested_fields = ex.split(".")
                        current_elem = elem
                        for f in nested_fields[:-1]:
                            if f in current_elem:
                                current_elem = current_elem[f]
                        field_to_exclude = nested_fields[-1]
                        if field_to_exclude in current_elem:
                            current_elem.pop(field_to_exclude, None)
                    else:  # handle top-level fields
                        elem["metadata"].pop(ex, None)  # type: ignore[attr-defined]
            elif self.config.metadata_include:
                in_list = self.config.metadata_include
                for k in list(elem["metadata"].keys()):  # type: ignore[attr-defined]
                    if k not in in_list:
                        elem["metadata"].pop(k, None)  # type: ignore[attr-defined]
            in_list = self.config.fields_include
            elem = {k: v for k, v in elem.items() if k in in_list}

            if self.config.flatten_metadata and "metadata" in elem:
                metadata = elem.pop("metadata")
                elem.update(flatten_dict(metadata, keys_to_omit=["data_source_record_locator"]))
        return element_dicts

    @requires_dependencies(dependencies=["unstructured"])
    def partition_locally(
        self, filename: Path, metadata: Optional[dict] = None, **kwargs
    ) -> list[dict]:
        from unstructured.documents.elements import DataSourceMetadata
        from unstructured.partition.auto import partition
        from unstructured.staging.base import elements_to_dicts

        @dataclass
        class FileDataSourceMetadata(DataSourceMetadata):
            filesize_bytes: Optional[int] = None

        logger.debug(f"using local partition with kwargs: {self.config.to_partition_kwargs()}")
        logger.debug(f"partitioning file {filename} with metadata {metadata}")
        elements = partition(
            filename=str(filename.resolve()),
            data_source_metadata=FileDataSourceMetadata.from_dict(metadata),
            **self.config.to_partition_kwargs(),
        )
        return self.postprocess(elements=elements_to_dicts(elements))

    @requires_dependencies(dependencies=["unstructured_client"], extras="remote")
    async def partition_via_api(
        self, filename: Path, metadata: Optional[dict] = None, **kwargs
    ) -> list[dict]:
        metadata = metadata or {}
        logger.debug(f"partitioning file {filename} with metadata: {metadata}")

        elements = await call_api_async(
            server_url=self.config.partition_endpoint,
            api_key=self.config.api_key.get_secret_value(),
            filename=filename,
            api_parameters=self.config.to_partition_kwargs(),
        )

        # Append the data source metadata the auto partition does for you
        for element in elements:
            element["metadata"]["data_source"] = metadata
        return self.postprocess(elements=elements)

    def run(self, filename: Path, metadata: Optional[dict] = None, **kwargs) -> list[dict]:
        return self.partition_locally(filename, metadata=metadata, **kwargs)

    async def run_async(
        self, filename: Path, metadata: Optional[dict] = None, **kwargs
    ) -> list[dict]:
        return await self.partition_via_api(filename, metadata=metadata, **kwargs)
