import json
import random
from typing import Any

import faker
import pytest

from test.unit.utils.data_generator import generate_random_dictionary
from unstructured_ingest.embed.vertexai import VertexAIEmbeddingConfig, VertexAIEmbeddingEncoder
from unstructured_ingest.processes.embedder import EmbedderConfig

fake = faker.Faker()

CREDENTIALS = json.dumps({"project_id": "test-project", "type": "service_account"})


def generate_embedder_config_params() -> dict:
    params = {
        "api_key": json.dumps(generate_random_dictionary(key_type=str, value_type=Any)),
    }
    if random.random() < 0.5:
        params["embedder_model_name"] = fake.word()
    return params


@pytest.mark.parametrize(
    "embedder_config_params", [generate_embedder_config_params() for i in range(10)]
)
def test_embedder_config(embedder_config_params: dict):
    embedder_config = VertexAIEmbeddingConfig.model_validate(embedder_config_params)
    assert embedder_config


@pytest.mark.parametrize(
    "embedder_config_params", [generate_embedder_config_params() for i in range(10)]
)
def test_embedder(embedder_config_params: dict):
    embedder_config = VertexAIEmbeddingConfig.model_validate(embedder_config_params)
    embedder = VertexAIEmbeddingEncoder(config=embedder_config)
    assert embedder


# --- EmbedderConfig pass-through ---------------------------------------------------------------


def test_embedder_config_forwards_gemini_settings():
    """The Gemini-only settings must survive the generic EmbedderConfig path, which is how the
    pipeline builds embedders."""
    embedder = EmbedderConfig(
        embedding_provider="vertexai",
        embedding_api_key=CREDENTIALS,
        embedding_model_name="gemini-embedding-2",
        embedding_vertexai_region="europe-west4",
        embedding_vertexai_dimensionality=768,
        embedding_vertexai_task="RETRIEVAL_DOCUMENT",
    ).get_embedder()

    assert embedder.config.region == "europe-west4"
    assert embedder.config.dimensionality == 768
    assert embedder.config.task == "RETRIEVAL_DOCUMENT"


def test_embedder_config_forwards_zero_dimensionality():
    """0 must survive the forwarding filter: it is a configured width, not "unset". A truthiness
    filter here would drop it and silently fall back to the model's full width."""
    embedder = EmbedderConfig(
        embedding_provider="vertexai",
        embedding_api_key=CREDENTIALS,
        embedding_model_name="gemini-embedding-2",
        embedding_vertexai_dimensionality=0,
    ).get_embedder()

    assert embedder.config.dimensionality == 0


def test_embedder_config_omits_unset_gemini_settings():
    """Unset settings must not be forwarded as explicit values, so the config's own defaults —
    including the VERTEXAI_REGION fallback — stay reachable."""
    embedder = EmbedderConfig(
        embedding_provider="vertexai",
        embedding_api_key=CREDENTIALS,
        embedding_model_name="gemini-embedding-2",
    ).get_embedder()

    assert embedder.config.region is None
    assert embedder.config.dimensionality is None
    assert embedder.config.task is None


def test_embedder_config_leaves_region_env_fallback_intact(monkeypatch):
    """The end-to-end consequence of not forwarding an unset region: $VERTEXAI_REGION still wins,
    so a pipeline that never sets the region keeps reaching its configured region."""
    monkeypatch.setenv("VERTEXAI_REGION", "us-east1")
    embedder = EmbedderConfig(
        embedding_provider="vertexai",
        embedding_api_key=CREDENTIALS,
        embedding_model_name="gemini-embedding-2",
    ).get_embedder()

    assert embedder.config._resolve_location() == "us-east1"


def test_embedder_config_region_overrides_env(monkeypatch):
    monkeypatch.setenv("VERTEXAI_REGION", "us-east1")
    embedder = EmbedderConfig(
        embedding_provider="vertexai",
        embedding_api_key=CREDENTIALS,
        embedding_model_name="gemini-embedding-2",
        embedding_vertexai_region="europe-west4",
    ).get_embedder()

    assert embedder.config._resolve_location() == "europe-west4"
