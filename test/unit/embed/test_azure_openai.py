import importlib.util

import httpx
import pytest

from unstructured_ingest.embed.azure_openai import AzureOpenAIEmbeddingConfig
from unstructured_ingest.error import UserAuthError, UserError

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("openai") is None,
    reason="openai extra not installed",
)


def _config() -> AzureOpenAIEmbeddingConfig:
    # `model_name` is the deployment name on Azure (deliberately not a base-model id).
    return AzureOpenAIEmbeddingConfig(
        api_key="key",
        azure_endpoint="https://example-resource.openai.azure.com/",
        model_name="myprefix-ada-3-small",
    )


def _api_error(exc_cls, status_code: int, code: str, message: str):
    """Build a realistic openai APIStatusError subclass with a JSON error body."""
    response = httpx.Response(
        status_code,
        request=httpx.Request("POST", "https://example-resource.openai.azure.com/"),
        json={"error": {"code": code, "message": message}},
    )
    return exc_cls(message, response=response, body={"code": code, "message": message})


def test_run_precheck_validates_deployment_via_embeddings_call(mocker):
    """Happy path: the precheck issues a minimal embeddings request against the
    deployment name and passes when the call succeeds."""
    mock_client = mocker.MagicMock()
    mocker.patch.object(AzureOpenAIEmbeddingConfig, "get_client", return_value=mock_client)

    _config().run_precheck()

    mock_client.embeddings.create.assert_called_once_with(
        input="precheck", model="myprefix-ada-3-small"
    )


def test_run_precheck_does_not_call_models_list(mocker):
    """Core of the fix: precheck must NOT validate against client.models.list(), whose
    Azure catalog never contains custom deployment names."""
    mock_client = mocker.MagicMock()
    mocker.patch.object(AzureOpenAIEmbeddingConfig, "get_client", return_value=mock_client)

    _config().run_precheck()

    mock_client.models.list.assert_not_called()


def test_run_precheck_deployment_not_found_raises_user_error(mocker):
    """A genuinely missing deployment surfaces as a UserError naming the deployment."""
    from openai import NotFoundError

    mock_client = mocker.MagicMock()
    mock_client.embeddings.create.side_effect = _api_error(
        NotFoundError,
        status_code=404,
        code="DeploymentNotFound",
        message="The API deployment for this resource does not exist.",
    )
    mocker.patch.object(AzureOpenAIEmbeddingConfig, "get_client", return_value=mock_client)

    with pytest.raises(UserError) as exc_info:
        _config().run_precheck()

    assert "myprefix-ada-3-small" in str(exc_info.value)


def test_run_precheck_auth_error_is_not_swallowed(mocker):
    """Regression: a non-404 APIStatusError (e.g. bad key) must be wrapped and raised,
    not silently treated as a passing precheck."""
    from openai import AuthenticationError

    mock_client = mocker.MagicMock()
    mock_client.embeddings.create.side_effect = _api_error(
        AuthenticationError,
        status_code=401,
        code="401",
        message="Access denied due to invalid subscription key.",
    )
    mocker.patch.object(AzureOpenAIEmbeddingConfig, "get_client", return_value=mock_client)

    with pytest.raises(UserAuthError):
        _config().run_precheck()


def test_run_precheck_unexpected_error_is_wrapped(mocker):
    """A non-API exception (e.g. connection failure) is routed through wrap_error."""
    mock_client = mocker.MagicMock()
    mock_client.embeddings.create.side_effect = ValueError("boom")
    mocker.patch.object(AzureOpenAIEmbeddingConfig, "get_client", return_value=mock_client)

    # wrap_error re-raises non-APIStatusError exceptions as-is rather than letting the
    # precheck pass silently.
    with pytest.raises(ValueError, match="boom"):
        _config().run_precheck()
