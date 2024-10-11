import click
from deltalake import DeltaTable


@click.command()
@click.option("--table-uri", type=str)
@click.option("--aws-access-key-id", type=str)
@click.option("--aws-secret-access-key", type=str)
def run_check(table_uri, aws_access_key_id, aws_secret_access_key):
    print(f"Checking contents of table at {table_uri}")
    if aws_access_key_id and aws_secret_access_key:
        storage_options = {
            "AWS_ACCESS_KEY_ID": aws_access_key_id,
            "AWS_SECRET_ACCESS_KEY": aws_secret_access_key,
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }
    else:
        storage_options = {}
    delta_table = DeltaTable(table_uri=table_uri, storage_options=storage_options)

    df = delta_table.to_pandas()
    EXPECTED_ROWS = 5
    EXPECTED_COLUMNS = 19
    print(f"Number of rows in table vs expected: {len(df)}/{EXPECTED_ROWS}")
    print(f"Number of columns in table vs expected: {len(df.columns)}/{EXPECTED_COLUMNS}")
    number_of_rows = len(df)
    assert number_of_rows == EXPECTED_ROWS, (
        f"number of rows in generated table ({number_of_rows}) "
        f"doesn't match expected value: {EXPECTED_ROWS}"
    )

    """
    The number of columns is associated with the flattened JSON structure of the partition output.
    If this changes, it's most likely due to the metadata changing in the output.
    """
    number_of_columns = len(df.columns)
    assert number_of_columns == EXPECTED_COLUMNS, (
        f"number of columns in generated table ({number_of_columns}) doesn't "
        f"match expected value: {EXPECTED_COLUMNS}"
    )
    print("table check complete")


if __name__ == "__main__":
    run_check()
