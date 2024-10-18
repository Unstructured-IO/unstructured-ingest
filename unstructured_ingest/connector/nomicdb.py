import multiprocessing as mp
import typing as t
from dataclasses import dataclass

from unstructured_ingest.enhanced_dataclass import enhanced_field
from unstructured_ingest.error import DestinationConnectionError, WriteError
from unstructured_ingest.interfaces import (
    AccessConfig,
    BaseConnectorConfig,
    BaseDestinationConnector,
    ConfigSessionHandleMixin,
    IngestDocSessionHandleMixin,
    WriteConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.data_prep import batch_generator, flatten_dict
from unstructured_ingest.utils.dep_check import requires_dependencies
import nomic
from nomic import atlas

if t.TYPE_CHECKING:
    from nomic import AtlasDataset

@dataclass 
class NomicAccessConfig(AccessConfig):
    api_key: t.Optional[str] = enhanced_field(sensitive=True)

@dataclass
class SimpleNomicConfig(ConfigSessionHandleMixin, BaseConnectorConfig):
    organisation_name: str
    dataset_name: str
    description: str
    domain: t.Optional[str] = None
    tenant: t.Optional[str] = None
    is_public: t.Optional[bool] = False
    access_config: t.Optional[NomicAccessConfig] = None

@dataclass
class NomicWriteConfig(WriteConfig):
    batch_size: int = 50
    num_processes: int = 1

@dataclass
class NomicDestinationConnector(IngestDocSessionHandleMixin, BaseDestinationConnector):
    write_config: NomicWriteConfig
    connector_config: SimpleNomicConfig
    _dataset: t.Optional["AtlasDataset"] = None

    @property
    def nomic_dataset(self):
        if self._dataset is None:
            self._dataset = self.create_dataset()
        return self._dataset

    def initialize(self):
        nomic.cli.login(
            token=self.connector_config.access_config.api_key, 
            domain=self.connector_config.domain, 
            tenant=self.connector_config.tenant
        )

    @requires_dependencies(["nomic"], extras="nomic")
    def create_dataset(self) -> "AtlasDataset":
        from nomic import AtlasDataset

        dataset = AtlasDataset(
            identifier=f"{self.connector_config.organisation_name}/{self.connector_config.dataset_name}",
            unique_id_field='element_id',
            description=self.connector_config.description,
            is_public=self.connector_config.is_public,
        )

        return dataset

    @DestinationConnectionError.wrap
    def check_connection(self):
        nomic.cli.login(
            token=self.connector_config.access_config.api_key, domain=self.connector_config.domain, tenant=self.connector_config.tenant
        )

    @DestinationConnectionError.wrap
    @requires_dependencies(["nomic"], extras="nomic")
    def upsert_batch(self, batch: t.List[t.Dict[str,t.Any]]):
        dataset = self.nomic_dataset
        try: 
            dataset.add_data(list(batch))
            # logger.debug(f"Successfully add {len(batch)} into dataset {dataset.id}")
        except Exception as api_error:
            raise WriteError(f"Nomic error: {api_error}") from api_error

    def write_dict(self, *args, elements_dict: t.List[t.Dict[str, t.Any]], **kwargs) -> None:
        logger.info(
            f"Upserting {len(elements_dict)} elements to "
            f"{self.connector_config.organisation_name}",
        )

        nomicdb_batch_size = self.write_config.batch_size

        logger.info(f'using {self.write_config.num_processes} processes to upload')
        if self.write_config.num_processes == 1:            
            for chunk in batch_generator(elements_dict, nomicdb_batch_size):
                self.upsert_batch(chunk)
        else:
            with mp.Pool(
                processes=self.write_config.num_processes,
            ) as pool:
                pool.map(self.upsert_batch, list(batch_generator(elements_dict, nomicdb_batch_size)))

        dataset = self.nomic_dataset
        dataset.create_index(
            indexed_field='text',
            topic_model=True,
            duplicate_detection=True,
            projection=None
        )



    def normalize_dict(self, element_dict: dict) -> dict:
        return {
            "element_id": element_dict['element_id'],
            "text": element_dict['text'],
            "type": element_dict['type'],
            "filename": element_dict['metadata']['filename']
        } 