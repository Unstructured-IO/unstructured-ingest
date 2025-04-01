import os

import pandas as pd
import pyarrow as pa
import requests
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.io.pyarrow import pyarrow_to_schema
from pyiceberg.table.name_mapping import NameMapping, MappedField
from pyiceberg.transforms import IdentityTransform

# Secrets
IBM_IAM_API_KEY = os.environ.get("IBM_IAM_API_KEY")
IBM_COS_ACCESS_KEY_ID = os.environ.get("IBM_COS_ACCESS_KEY_ID")
IBM_COS_SECRET_ACCESS_KEY = os.environ.get("IBM_COS_SECRET_ACCESS_KEY")

IBM_COS_ENDPOINT = os.environ.get("IBM_COS_ENDPOINT")
IBM_COS_REGION = "us"
ICEBERG_ENDPOINT = os.environ.get("ICEBERG_ENDPOINT")
ICEBERG_CATALOG = "iceberg_data"

# Get Bearer Token required for Iceberg REST access
bearer_token = requests.post(
    "https://iam.cloud.ibm.com/identity/token",
    headers={
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    },
    data={
        "grant_type": "urn:ibm:params:oauth:grant-type:apikey",
        "apikey": IBM_IAM_API_KEY,
    },
).json()["access_token"]

# Configure Iceberg catalog connection
catalog = load_catalog(
    ICEBERG_CATALOG,
    **{
        "type": "rest",
        "uri": f"https://{ICEBERG_ENDPOINT}/mds/iceberg",
        "token": bearer_token,
        "warehouse": ICEBERG_CATALOG,
        "s3.endpoint": f"https://{IBM_COS_ENDPOINT}",
        "s3.access-key-id": IBM_COS_ACCESS_KEY_ID,
        "s3.secret-access-key": IBM_COS_SECRET_ACCESS_KEY,
        "s3.region": IBM_COS_REGION,
    },
)

# # Load sample partitioned data into DataFrame
elements_json = "downloads/for-iceberg/99eb1672fb0a.json"
df = pd.read_json(elements_json)
df.drop(columns=["languages"], inplace=True)
print(f"Number of records to append: {len(df)}")

# # Transform DataFrame to Arrow Table
pa_df = pa.Table.from_pandas(df)

# Create namespace if not exists
namespace_name = "test-ingest"
catalog.create_namespace_if_not_exists(namespace_name)

# List namespaces
namespaces = catalog.list_namespaces()
print(f"Available namespaces: {namespaces}")

# Create mapping to map column name to unique integer
name_mapping = NameMapping(
    [
        MappedField(**{"field-id": i, "names": [name]})
        for i, name in enumerate(pa_df.column_names)
    ]
)

# Create table if not exists
table_schema = pyarrow_to_schema(pa_df.schema, name_mapping=name_mapping)
table_name = "elements_4"
table_identifier = (namespace_name, table_name)
partition_field = table_schema.find_field(
    "record_id"
)
partition_spec = PartitionSpec(
    PartitionField(
        source_id=partition_field.field_id,
        field_id=partition_field.field_id,
        name="record_id",
        transform=IdentityTransform()
    )
)
print(f"{".".join(table_identifier)} schema: \n{table_schema}")
catalog.create_table_if_not_exists(
    identifier=table_identifier, schema=table_schema, partition_spec=partition_spec
)

# # Append data to table
# table = catalog.load_table(table_identifier)
# table.append(pa_df)
# print(f"Number of records after appending: {len(table.scan().to_arrow())}")
