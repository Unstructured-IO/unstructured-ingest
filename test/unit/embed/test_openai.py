import pytest
from pydantic import SecretStr, ValidationError

from unstructured_ingest.embed.openai import (
    CustomOpenAICompatibleEmbeddingConfig,
    OpenAIEmbeddingConfig,
    OpenAIEmbeddingEncoder,
)


def test_embed_documents_does_not_break_element_to_dict(mocker):
    # Mocked client with the desired behavior for embed_documents
    raw_elements = [{"text": f"This is sentence {i + 1}"} for i in range(4)]
    mock_response = mocker.MagicMock()
    mock_response_data = []
    for i in range(2):
        mock_response_d = mocker.MagicMock()
        mock_response_d.embedding = [1, 2]
        mock_response_data.append(mock_response_d)
    mock_response.data = mock_response_data
    mock_client = mocker.MagicMock()
    mock_client.embeddings.create.return_value = mock_response

    # Mock get_client to return our mock_client
    mocker.patch.object(OpenAIEmbeddingConfig, "get_client", return_value=mock_client)

    encoder = OpenAIEmbeddingEncoder(config=OpenAIEmbeddingConfig(api_key="api_key", batch_size=2))

    elements = encoder.embed_documents(
        elements=raw_elements,
    )
    assert len(elements) == 4
    assert elements[0]["text"] == "This is sentence 1"
    assert elements[1]["text"] == "This is sentence 2"
    assert mock_client.embeddings.create.call_count == 2


class TestCustomOpenAICompatibleEmbeddingConfig:
    @pytest.mark.parametrize("get_client_method", ["get_client", "get_async_client"])
    def test_without_api_key_omits_authorization(self, get_client_method):
        config = CustomOpenAICompatibleEmbeddingConfig(base_url="http://fake/v1")
        client = getattr(config, get_client_method)()
        assert not client.auth_headers

    @pytest.mark.parametrize("get_client_method", ["get_client", "get_async_client"])
    def test_with_api_key_emits_bearer(self, get_client_method):
        config = CustomOpenAICompatibleEmbeddingConfig(
            api_key=SecretStr("testkey"), base_url="http://fake/v1"
        )
        client = getattr(config, get_client_method)()
        assert client.auth_headers == {"Authorization": "Bearer testkey"}

    @pytest.mark.parametrize("get_client_method", ["get_client", "get_async_client"])
    def test_default_headers_plaintext_on_wire(self, get_client_method):
        config = CustomOpenAICompatibleEmbeddingConfig(
            base_url="http://fake/v1",
            default_headers={"X-Custom": SecretStr("tok")},
        )
        client = getattr(config, get_client_method)()
        assert client.default_headers.get("X-Custom") == "tok"

    def test_rejects_authorization_in_default_headers(self):
        for key in ["Authorization", "authorization", "AUTHORIZATION", "aUtHoRiZaTiOn"]:
            with pytest.raises(ValidationError):
                CustomOpenAICompatibleEmbeddingConfig(
                    base_url="http://fake/v1",
                    default_headers={key: SecretStr("bad")},
                )

    def test_empty_dict_default_headers(self, mocker):
        mock_openai = mocker.patch("openai.OpenAI")
        mocker.patch("openai.DefaultHttpxClient")
        config = CustomOpenAICompatibleEmbeddingConfig(
            api_key=SecretStr("k"), base_url="http://fake/v1", default_headers={}
        )
        config.get_client()
        assert mock_openai.call_args.kwargs.get("default_headers") is None

    def test_json_round_trip(self):
        config = CustomOpenAICompatibleEmbeddingConfig(
            base_url="http://fake/v1",
            default_headers={"X-Custom": SecretStr("tok")},
        )
        json_str = config.model_dump_json()
        rehydrated = CustomOpenAICompatibleEmbeddingConfig.model_validate_json(json_str)
        client = rehydrated.get_client()
        assert client.default_headers.get("X-Custom") == "tok"
