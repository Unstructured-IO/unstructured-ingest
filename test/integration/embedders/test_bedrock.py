import json
import os
from pathlib import Path

from test.integration.embedders.utils import validate_embedding_output
from test.integration.utils import requires_env
from unstructured_ingest.v2.processes.embedder import Embedder, EmbedderConfig


def get_aws_credentials() -> dict:
    access_key = os.getenv("AWS_ACCESS_KEY_ID", None)
    assert access_key
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", None)
    assert secret_key
    return {"aws_access_key_id": access_key, "aws_secret_access_key": secret_key}


@requires_env("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
def test_bedrock_embedder(embedder_file: Path):
    aws_credentials = get_aws_credentials()
    embedder_config = EmbedderConfig(
        embedding_provider="aws-bedrock",
        embedding_aws_access_key_id=aws_credentials["aws_access_key_id"],
        embedding_aws_secret_access_key=aws_credentials["aws_secret_access_key"],
    )
    embedder = Embedder(config=embedder_config)
    results = embedder.run(elements_filepath=embedder_file)
    assert results
    with embedder_file.open("r") as f:
        original_elements = json.load(f)
    validate_embedding_output(original_elements=original_elements, output_elements=results)
