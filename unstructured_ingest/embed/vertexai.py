# type: ignore
import asyncio
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Optional

from pydantic import Field, Secret, ValidationError, model_validator
from pydantic.functional_validators import BeforeValidator

from unstructured_ingest.embed.interfaces import (
    AsyncBaseEmbeddingEncoder,
    BaseEmbeddingEncoder,
    EmbeddingConfig,
)
from unstructured_ingest.error import (
    ProviderError,
    QuotaError,
    UserAuthError,
    UserError,
    is_internal_error,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from vertexai.language_models import TextEmbeddingModel


def conform_string_to_dict(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    raise ValidationError(f"Input could not be mapped to a valid dict: {value}")


ApiKeyType = Secret[Annotated[dict, BeforeValidator(conform_string_to_dict)]]


# Gemini-family embedding models are NOT served through the PaLM-era
# ``vertexai.language_models.TextEmbeddingModel`` predict interface. That class dispatches on the
# publisher model's ``predict_schemata.instance_schema_uri`` and only accepts
# ``.../text_embedding_1.0.0.yaml``, so ``TextEmbeddingModel.from_pretrained("gemini-embedding-2")``
# raises ``ValueError: Unknown model publishers/google/models/gemini-embedding-2; {...}``. The
# Gemini family is reached through google-genai's ``embed_content`` instead.
#
# Dispatch on the model-id prefix rather than probing the publisher model: it costs no extra
# authenticated round trip, and new members of the family (``gemini-embedding-001``,
# ``gemini-embedding-2``, ...) work without a release here. Matched against the bare model id, so
# a publisher-style path naming a Gemini model routes here too — see _vertex_routing_model_id.
_GENAI_MODEL_PREFIXES: tuple[str, ...] = ("gemini-embedding",)

# ``embedContent`` accepts exactly ONE content per request for these models — passing a list of
# strings does NOT batch, it is coalesced into a single multi-part Content and returns a single
# vector for the whole batch (an explicit ``list[Content]`` fails outright with "The embedContent
# API for this model only supports one content at a time"). So a batch is fanned out as one request
# per text. The async path runs them concurrently, bounded to keep a large ``batch_size`` from
# turning into an equally large burst of requests against the provider's rate limit.
_GENAI_MAX_CONCURRENT_REQUESTS = 8

# Vertex models can also be addressed by a full resource path that already names the region they are
# served from, e.g. ``projects/<project>/locations/<location>/publishers/google/models/<model>`` for
# a publisher model, or ``projects/<project>/locations/<location>/endpoints/<id>`` for a fine-tuned
# endpoint. Those live in a fixed location, so a single global ``VERTEXAI_REGION`` cannot reach more
# than one of them in the same process — and the client must be built for the location named in the
# path, or the request 404s. Plain model ids (``gemini-embedding-2``) do not match and keep using
# the configured/global region.
_VERTEX_RESOURCE_LOCATION_RE = re.compile(
    r"^projects/(?P<project>[^/]+)/locations/(?P<location>[^/]+)/"
)

# A publisher-style resource path names the model in its final segment, so transport routing can
# still tell a Gemini model from a legacy one. A bare ``endpoints/<id>`` path names no model at all,
# carries no signal about which family it serves, and therefore stays on the legacy transport.
_VERTEX_PUBLISHER_MODEL_RE = re.compile(
    r"^projects/[^/]+/locations/[^/]+/publishers/[^/]+/models/(?P<model>.+)$"
)

# Vertex multi-region locations are not served by the ``<location>-aiplatform.googleapis.com``
# regional host google-genai builds by default (that host is invalid for a multi-region) — they are
# served by the regionalized "rep" host. Supplied explicitly so a model deployed to a multi-region
# endpoint is reachable without depending on the installed SDK version building it.
_VERTEX_MULTI_REGIONS = frozenset({"us", "eu"})

# Region of last resort for the genai transport. Availability is per-model, not per-deployment: the
# Gemini embedding family is served from ``global`` and 404s in ordinary regions like us-east1,
# while the legacy text-embedding models are served from both. So a deployment-wide regional pin
# cannot be correct for every model, and ``global`` is the only value that serves all of them.
# NOTE: ``global`` is deliberately not in _VERTEX_MULTI_REGIONS — google-genai builds the correct
# host for it on its own, and forcing the "rep" host here would break it.
_VERTEX_DEFAULT_LOCATION = "global"

# Workload identity field rules, the same as the platform's provider config: the region and project
# form the Vertex host and path, and only user-managed service accounts may be impersonated.
VERTEX_WORKLOAD_IDENTITY_RULES: dict[str, tuple[re.Pattern, str]] = {
    "region": (
        re.compile(r"global|us|eu|[a-z]+-[a-z]+[0-9]+"),
        "vertexai: region must be global, us, eu or a region such as us-central1",
    ),
    "project_id": (
        re.compile(r"[a-z][a-z0-9-]{4,28}[a-z0-9]"),
        "vertexai: project_id must be a Google Cloud project ID",
    ),
    "service_account_email": (
        re.compile(
            r"[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z][a-z0-9-]{4,28}[a-z0-9]\.iam\.gserviceaccount\.com"
        ),
        "vertexai: service_account_email must be a user-managed service account "
        "(<name>@<project>.iam.gserviceaccount.com)",
    ),
}


def _vertex_location_from_resource_path(model_name: Optional[str]) -> Optional[str]:
    """Return the ``locations/<loc>`` segment of a full Vertex resource-path model id, or ``None``
    for a plain model id (which should fall back to the configured region)."""
    if not isinstance(model_name, str):
        return None
    match = _VERTEX_RESOURCE_LOCATION_RE.match(model_name)
    return match.group("location") if match else None


def _vertex_routing_model_id(model_name: Optional[str]) -> str:
    """Return the bare model id to route on. Unwraps a publisher-style resource path
    (``projects/../locations/../publishers/google/models/gemini-embedding-2`` -> that final
    segment); any other value, including a bare ``endpoints/<id>`` path, is returned unchanged."""
    if not isinstance(model_name, str):
        return ""
    match = _VERTEX_PUBLISHER_MODEL_RE.match(model_name)
    return match.group("model") if match else model_name


def _vertex_multi_region_base_url(location: Optional[str]) -> Optional[str]:
    """Return the regionalized "rep" base URL for a Vertex multi-region location, else ``None``."""
    if location in _VERTEX_MULTI_REGIONS:
        return f"https://aiplatform.{location}.rep.googleapis.com/"
    return None


class VertexAIEmbeddingConfig(EmbeddingConfig):
    api_key: Optional[ApiKeyType] = Field(
        default=None,
        description="API key for Vertex AI. Omit to use workload identity "
        "(service_account_email and project_id).",
    )
    service_account_email: Optional[str] = Field(
        default=None,
        description="Service account to impersonate with workload identity, instead of an API key",
    )
    project_id: Optional[str] = Field(
        default=None, description="Google Cloud project for Vertex AI calls with workload identity"
    )
    embedder_model_name: Optional[str] = Field(
        default="text-embedding-005", alias="model_name", description="Vertex AI model name"
    )
    region: Optional[str] = Field(
        default=None,
        description=(
            "Vertex AI region. Only used by Gemini-family models; falls back to the "
            "VERTEXAI_REGION environment variable, then to 'global'. Required with workload "
            "identity, which has no fallback."
        ),
    )
    dimensionality: Optional[int] = Field(
        default=None,
        description=(
            "Output embedding dimension. Only used by Gemini-family models, which support "
            "Matryoshka truncation; ignored by the legacy text-embedding models."
        ),
    )
    task: Optional[str] = Field(
        default=None,
        description=(
            "Embedding task type, e.g. RETRIEVAL_DOCUMENT. Only used by Gemini-family models."
        ),
    )

    @model_validator(mode="after")
    def _validate_auth(self) -> "VertexAIEmbeddingConfig":
        has_identity = bool(self.service_account_email) or bool(self.project_id)
        if self.api_key is not None and has_identity:
            raise ValueError("set either api_key or service_account_email and project_id, not both")
        if self.api_key is not None:
            return self
        if not (self.service_account_email and self.project_id):
            raise ValueError("api_key, or both service_account_email and project_id, is required")
        for name, (pattern, message) in VERTEX_WORKLOAD_IDENTITY_RULES.items():
            value = (getattr(self, name) or "").strip()
            if not pattern.fullmatch(value):
                raise ValueError(message)
            setattr(self, name, value)
        return self

    @property
    def uses_genai_transport(self) -> bool:
        """Whether this model must be driven through google-genai rather than TextEmbeddingModel."""
        if self.api_key is None:
            # The legacy transport reads a key file from the environment.
            return True
        model_name = _vertex_routing_model_id(self.embedder_model_name).lower()
        return model_name.startswith(_GENAI_MODEL_PREFIXES)

    def wrap_error(self, e: Exception) -> Exception:
        if is_internal_error(e=e):
            return e
        from google.api_core.exceptions import ResourceExhausted
        from google.auth.exceptions import GoogleAuthError

        if isinstance(e, GoogleAuthError):
            return UserAuthError(e)
        if isinstance(e, ResourceExhausted):
            # Quota / rate exhaustion (HTTP 429). Surface as QuotaError - a user
            # error - so it fails loudly instead of being mistaken for a
            # transient provider blip and skipped. Mirrors OpenAI/OctoAI, which
            # map insufficient_quota to QuotaError.
            return QuotaError(e)
        # The Gemini-family transport goes through google-genai, which raises its OWN
        # google.genai.errors.ClientError (code 429) for quota exhaustion, NOT
        # api_core.ResourceExhausted - so without this a Gemini 429 would surface raw and the
        # transient-skip guard (which treats a 429 as transient) would swallow a genuine quota
        # exhaustion. Map it to QuotaError too. Imported defensively: google-genai is only present
        # with the `vertexai` extra, and the legacy transport never raises this type.
        try:
            from google.genai.errors import ClientError as GenAIClientError
        except ImportError:
            GenAIClientError = None
        if (
            GenAIClientError is not None
            and isinstance(e, GenAIClientError)
            and getattr(e, "code", None) == 429
        ):
            return QuotaError(e)
        return e

    def register_application_credentials(self):
        # TODO look into passing credentials in directly, rather than via env var and tmp file
        application_credentials_path = Path("/tmp") / "google-vertex-app-credentials.json"
        with application_credentials_path.open("w+") as credentials_file:
            json.dump(self.api_key.get_secret_value(), credentials_file)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(application_credentials_path)

    def _resolve_location(self) -> str:
        """Region for the genai client: the location baked into a full resource-path model id, else
        the configured region, else ``$VERTEXAI_REGION``, else ``global``."""
        return (
            _vertex_location_from_resource_path(self.embedder_model_name)
            or self.region
            or os.getenv("VERTEXAI_REGION")
            or _VERTEX_DEFAULT_LOCATION
        )

    @requires_dependencies(["google.genai"], extras="vertexai")
    def get_genai_client(self):
        """Creates a google-genai client in Vertex mode for Gemini-family embedding models."""
        from google import genai

        if self.api_key is None:
            project, credentials = self.project_id, self._workload_identity_credentials()
        else:
            from google.oauth2 import service_account

            credentials_json = self.api_key.get_secret_value()
            project = credentials_json.get("project_id")
            if not project:
                raise UserError(
                    "Vertex AI credentials are missing 'project_id'; a service account JSON is "
                    "required for Gemini-family embedding models."
                )
            credentials = service_account.Credentials.from_service_account_info(
                credentials_json, scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
        location = self._resolve_location()
        client_kwargs: dict[str, Any] = {}
        base_url = _vertex_multi_region_base_url(location)
        if base_url is None and self.api_key is None:
            # An explicit host, so GOOGLE_VERTEX_BASE_URL or a process-wide default cannot
            # redirect the impersonated token.
            base_url = (
                "https://aiplatform.googleapis.com/"
                if location == _VERTEX_DEFAULT_LOCATION
                else f"https://{location}-aiplatform.googleapis.com/"
            )
        if base_url:
            client_kwargs["http_options"] = genai.types.HttpOptions(base_url=base_url)
        return genai.Client(
            vertexai=True,
            credentials=credentials,
            project=project,
            location=location,
            **client_kwargs,
        )

    def _workload_identity_credentials(self) -> Any:
        from unstructured_ingest.embed.vertexai_identity import (
            refusal,
            vertex_workload_identity_credentials,
        )

        # The resource path picks the host, so it must stay in the configured project and pass
        # the region rule before the impersonated token goes anywhere.
        model_name = self.embedder_model_name or ""
        scope = _VERTEX_RESOURCE_LOCATION_RE.match(model_name)
        region_rule = VERTEX_WORKLOAD_IDENTITY_RULES["region"][0]
        if model_name.startswith("projects/") and not (
            scope
            and scope.group("project") == self.project_id
            and region_rule.fullmatch(scope.group("location"))
        ):
            raise refusal(
                f"Model {self.embedder_model_name} is not in project {self.project_id}.",
                "vertex_model_outside_project",
            )
        return vertex_workload_identity_credentials(self.service_account_email)

    def run_precheck(self) -> None:
        """Embed one input with workload identity, which proves the whole token chain, the
        Vertex AI role and the model. Key configs are not prechecked."""
        if self.api_key is not None:
            return
        try:
            client = self.get_genai_client()
            self._genai_embed_one(client, "precheck", self._genai_embed_config())
        except Exception as e:
            raise self.wrap_error(e=e)

    @requires_dependencies(["vertexai"], extras="vertexai")
    def get_legacy_client(self) -> "TextEmbeddingModel":
        """Creates a VertexAI python client to embed elements."""
        from vertexai.language_models import TextEmbeddingModel

        self.register_application_credentials()
        return TextEmbeddingModel.from_pretrained(self.embedder_model_name)

    def get_client(self):
        """Client for the transport this model is served through."""
        if self.uses_genai_transport:
            return self.get_genai_client()
        return self.get_legacy_client()

    def _genai_embed_config(self):
        """``EmbedContentConfig`` for the configured dimensionality/task, or ``None``."""
        from google.genai import types

        kwargs: dict[str, Any] = {}
        if self.dimensionality is not None:
            kwargs["output_dimensionality"] = self.dimensionality
        if self.task:
            kwargs["task_type"] = self.task
        return types.EmbedContentConfig(**kwargs) if kwargs else None

    @staticmethod
    def _first_embedding(response: Any) -> list[float]:
        """Values of the single embedding an ``embedContent`` request returns."""
        embeddings = response.embeddings
        if not embeddings:
            raise ProviderError("Vertex AI embedContent returned no embedding for an input")
        return embeddings[0].values

    def _genai_embed_one(self, client: Any, text: str, config: Any) -> list[float]:
        response = client.models.embed_content(
            model=self.embedder_model_name, contents=text, config=config
        )
        return self._first_embedding(response)

    # Batch embedding lives on the config, not the encoder, so that every caller — the sync
    # encoder, the async encoder, and downstream callers that drive their own batch loop — shares
    # one implementation and cannot drift on which transport a model needs. Dependencies are
    # already gated by the matching ``get_*_client``, which must run before a client gets here.
    def embed_batch(self, client: Any, batch: list[str]) -> list[list[float]]:
        if self.uses_genai_transport:
            config = self._genai_embed_config()
            return [self._genai_embed_one(client, text, config) for text in batch]

        from vertexai.language_models import TextEmbeddingInput

        inputs = [TextEmbeddingInput(text=text) for text in batch]
        response = client.get_embeddings(inputs)
        return [e.values for e in response]

    async def embed_batch_async(self, client: Any, batch: list[str]) -> list[list[float]]:
        if self.uses_genai_transport:
            config = self._genai_embed_config()
            semaphore = asyncio.Semaphore(_GENAI_MAX_CONCURRENT_REQUESTS)

            async def embed_one(text: str) -> list[float]:
                async with semaphore:
                    response = await client.aio.models.embed_content(
                        model=self.embedder_model_name, contents=text, config=config
                    )
                return self._first_embedding(response)

            return list(await asyncio.gather(*(embed_one(text) for text in batch)))

        from vertexai.language_models import TextEmbeddingInput

        inputs = [TextEmbeddingInput(text=text) for text in batch]
        response = await client.get_embeddings_async(inputs)
        return [e.values for e in response]


@dataclass
class VertexAIEmbeddingEncoder(BaseEmbeddingEncoder):
    config: VertexAIEmbeddingConfig

    def precheck(self):
        self.config.run_precheck()

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    def get_client(self) -> Any:
        return self.config.get_client()

    def embed_batch(self, client: Any, batch: list[str]) -> list[list[float]]:
        return self.config.embed_batch(client=client, batch=batch)


@dataclass
class AsyncVertexAIEmbeddingEncoder(AsyncBaseEmbeddingEncoder):
    config: VertexAIEmbeddingConfig

    def precheck(self):
        self.config.run_precheck()

    def wrap_error(self, e: Exception) -> Exception:
        return self.config.wrap_error(e=e)

    def get_client(self) -> Any:
        return self.config.get_client()

    async def embed_batch(self, client: Any, batch: list[str]) -> list[list[float]]:
        return await self.config.embed_batch_async(client=client, batch=batch)
