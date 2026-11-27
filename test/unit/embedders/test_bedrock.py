import asyncio
import os
import random
from contextlib import asynccontextmanager
from unittest.mock import patch

import faker
import pytest
from botocore.exceptions import ClientError

from unstructured_ingest.embed.bedrock import (
    AsyncBedrockEmbeddingEncoder,
    BedrockEmbeddingConfig,
    BedrockEmbeddingEncoder,
)
from unstructured_ingest.error import ProviderError, RateLimitError, UserAuthError

fake = faker.Faker()


def generate_embedder_config_params() -> dict:
    params = {
        "aws_access_key_id": fake.password(),
        "aws_secret_access_key": fake.password(),
        "region_name": fake.city(),
    }
    if random.random() < 0.5:
        params["embedder_model_name"] = fake.word()
    return params


@pytest.mark.parametrize(
    "embedder_config_params", [generate_embedder_config_params() for i in range(10)]
)
def test_embedder_config(embedder_config_params: dict):
    embedder_config = BedrockEmbeddingConfig.model_validate(embedder_config_params)
    assert embedder_config


@pytest.mark.parametrize(
    "embedder_config_params", [generate_embedder_config_params() for i in range(10)]
)
def test_embedder(embedder_config_params: dict):
    embedder_config = BedrockEmbeddingConfig.model_validate(embedder_config_params)
    embedder = BedrockEmbeddingEncoder(config=embedder_config)
    assert embedder


def test_inference_profile_default():
    config = BedrockEmbeddingConfig(aws_access_key_id="test", aws_secret_access_key="test")
    assert config.inference_profile_id is None


@patch.dict(os.environ, {"BEDROCK_INFERENCE_PROFILE_ID": "test-profile"})
def test_inference_profile_env_var():
    config = BedrockEmbeddingConfig(aws_access_key_id="test", aws_secret_access_key="test")
    assert config.inference_profile_id == "test-profile"


def test_inference_profile_manual():
    config = BedrockEmbeddingConfig(
        aws_access_key_id="test",
        aws_secret_access_key="test",
        inference_profile_id="manual-profile",
    )
    assert config.inference_profile_id == "manual-profile"


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("model_name", ["amazon.titan-embed-text-v2:0", "cohere.embed-english-v3"])
@pytest.mark.parametrize(
    "profile",
    [
        None,
        "us.amazon.titan-embed-text-v2:0",
        "arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/test",
    ],
)
def test_invoke_model_with_inference_profile(
    asynchronous: bool, model_name: str, profile: str | None
):
    import json
    from io import BytesIO

    from botocore.session import Session
    from botocore.validate import validate_parameters

    config = BedrockEmbeddingConfig(model_name=model_name, inference_profile_id=profile)
    shape = (
        Session().get_service_model("bedrock-runtime").operation_model("InvokeModel").input_shape
    )
    cohere = model_name.startswith("cohere.")
    payload = {"embeddings": [[1.0]]} if cohere else {"embedding": [1.0]}
    calls = []

    def validate_request(params: dict) -> None:
        validate_parameters(params, shape)
        assert params["modelId"] == (profile or model_name)
        expected_body = (
            {"texts": ["hello"], "input_type": "search_document"}
            if cohere
            else {"inputText": "hello"}
        )
        assert json.loads(params["body"]) == expected_body
        calls.append(params)

    class SyncClient:
        def invoke_model(self, **params):
            validate_request(params)
            return {"body": BytesIO(json.dumps(payload).encode())}

    class AsyncBody:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def json(self):
            return payload

    class AsyncClient:
        async def invoke_model(self, **params):
            validate_request(params)
            return {"body": AsyncBody()}

    @asynccontextmanager
    async def get_async_client(self):
        yield AsyncClient()

    if asynchronous:
        with patch.object(BedrockEmbeddingConfig, "get_async_client", get_async_client):
            result = asyncio.run(AsyncBedrockEmbeddingEncoder(config=config).embed_query("hello"))
    else:
        with patch.object(BedrockEmbeddingConfig, "get_client", return_value=SyncClient()):
            result = BedrockEmbeddingEncoder(config=config).embed_query("hello")
    assert result == [1.0]
    assert len(calls) == 1


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    ("status", "code", "expected"),
    [
        (429, "ThrottlingException", RateLimitError),
        (403, "AccessDeniedException", UserAuthError),
        (500, "InternalServerException", ProviderError),
    ],
)
def test_invoke_model_client_error(
    asynchronous: bool, status: int, code: str, expected: type[Exception]
):
    error = ClientError(
        {"Error": {"Code": code, "Message": "x"}, "ResponseMetadata": {"HTTPStatusCode": status}},
        "InvokeModel",
    )

    class SyncClient:
        def invoke_model(self, **params):
            raise error

    class AsyncClient:
        async def invoke_model(self, **params):
            raise error

    @asynccontextmanager
    async def get_async_client(self):
        yield AsyncClient()

    config = BedrockEmbeddingConfig(model_name="amazon.titan-embed-text-v2:0")
    with pytest.raises(expected):
        if asynchronous:
            with patch.object(BedrockEmbeddingConfig, "get_async_client", get_async_client):
                asyncio.run(AsyncBedrockEmbeddingEncoder(config=config).embed_query("hello"))
        else:
            with patch.object(BedrockEmbeddingConfig, "get_client", return_value=SyncClient()):
                BedrockEmbeddingEncoder(config=config).embed_query("hello")
