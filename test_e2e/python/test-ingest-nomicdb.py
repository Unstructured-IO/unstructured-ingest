import os
from unstructured_client.models import operations, shared
from unstructured_ingest.connector.nomicdb import (
  NomicAccessConfig,
  NomicWriteConfig,
  SimpleNomicConfig
)

from unstructured_ingest.connector.local import SimpleLocalConfig

from unstructured_ingest.interfaces import PartitionConfig, ProcessorConfig, ReadConfig
from unstructured_ingest.runner import SharePointRunner, LocalRunner
from unstructured_ingest.runner.writers.base_writer import Writer
from unstructured_ingest.runner.writers.nomicdb import (
  NomicWriter,
)
from dotenv import load_dotenv
load_dotenv('./test_e2e/python/.env')

def get_writer(
  organisation_name: str,
  dataset_name: str,
  description: str,
  api_key: str,
  domain: str = "atlas.nomic.ai",
  tenant: str = "production",
  is_public: bool = False
) -> Writer:
  return NomicWriter(
    connector_config=SimpleNomicConfig(
      organisation_name=organisation_name,
      dataset_name=dataset_name,
      description=description,
      domain=domain,
      tenant=tenant,
      is_public=is_public,
      access_config=NomicAccessConfig(
        api_key=api_key
      ),
    ),
    write_config=NomicWriteConfig(
      num_processes=2,
      batch_size=80
    )
  )

def main():
  """
  parse data and ingest into nomicdb and construct nomic atlas map
  """
  writer = get_writer(
    organisation_name=os.getenv('NOMIC_ORG_NAME'), 
    dataset_name=os.getenv('NOMIC_ADS_NAME'),
    description='a dataset created by test-ingest-nomicdb',
    api_key=os.getenv('NOMIC_API_KEY'),
    is_public=True)

  runner = LocalRunner(
    processor_config=ProcessorConfig(
      verbose=True,
      output_dir="./data/local-ingest-output/",
      num_processes=2
    ),
    read_config=ReadConfig(),
    writer=writer,
    partition_config=PartitionConfig(
      partition_by_api=True,
      api_key=os.getenv("UNSTRUCTURED_API_KEY"),
      partition_endpoint=os.getenv("UNSTRUCTURED_API_URL"),
      strategy="auto",
    ),
    connector_config=SimpleLocalConfig(
        input_path='./example-docs/',
        recursive=False,
    ),      
  )
  runner.run()

if __name__ == '__main__':
    main()