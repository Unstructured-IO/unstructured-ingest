"""Keyless Vertex AI credentials: impersonate a customer service account through a broker.

The chain is the pod's own identity (the GKE metadata server, or a workload identity pool
exchange of a projected token), then one ``generateAccessToken`` per hop with no delegates:
pod -> broker -> customer service account. A failure is tagged with the stage that raised it,
so a broker hop failure (the deployment's) reads apart from a customer hop failure.

Same env contract, fixed URLs and stage tags as ``utic_auth.gcp.credentials``, which this
package cannot depend on. Never ``google.auth.default()`` and never a credentials file.
"""

import functools
import os
import re
from contextlib import contextmanager
from typing import Any, Iterator, Optional

from google.auth import compute_engine, identity_pool, impersonated_credentials
from google.auth import exceptions as google_exceptions
from google.auth.credentials import Credentials

from unstructured_ingest.embed.vertexai import VERTEX_WORKLOAD_IDENTITY_RULES
from unstructured_ingest.error import UserError
from unstructured_ingest.logger import logger

SOURCE_ENV = "GCP_WORKLOAD_IDENTITY_SOURCE"
BROKER_ENV = "GCP_WORKLOAD_IDENTITY_BROKER_SA"
AUDIENCE_ENV = "GCP_WORKLOAD_IDENTITY_AUDIENCE"
TOKEN_FILE_ENV = "GCP_FEDERATED_TOKEN_FILE"
ALLOWED_PROJECTS_ENV = "GCP_WORKLOAD_IDENTITY_ALLOWED_PROJECTS"

GKE_METADATA = "gke_metadata"
IDENTITY_POOL = "identity_pool"

STAGE_ATTRIBUTE = "vertex_identity_stage"
SOURCE_STAGE = "source"  # the pod's own identity: metadata server or STS
BROKER_STAGE = "broker"  # hop 1, pod -> broker: the deployment's grant
SERVICE_ACCOUNT_STAGE = "service_account"  # hop 2, broker -> customer SA: the customer's grant

_UNIVERSE_DOMAIN = "googleapis.com"
_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
_STS_TOKEN_URL = "https://sts.googleapis.com/v1/token"
_GENERATE_ACCESS_TOKEN_URL = (
    "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{}:generateAccessToken"
)
_SERVICE_ACCOUNT_SUFFIX = ".iam.gserviceaccount.com"
_SERVICE_ACCOUNT = VERTEX_WORKLOAD_IDENTITY_RULES["service_account_email"][0]
_AUDIENCE = re.compile(
    r"//iam\.googleapis\.com/projects/[0-9]+/locations/global/"
    r"workloadIdentityPools/[a-z0-9-]+/providers/[a-z0-9-]+"
)

_UNAVAILABLE_MESSAGE = (
    "Vertex AI workload identity is not available on this deployment. Contact your "
    "Unstructured account team."
)


def refusal(message: str, error_reason: str) -> UserError:
    """A ``UserError`` carrying the stable reason consumers classify it by."""
    error = UserError(message)
    error.error_reason = error_reason
    return error


def _unavailable(detail: str) -> UserError:
    # The detail names deployment settings only, for operators.
    logger.error("Vertex AI workload identity unavailable: %s", detail)
    return refusal(_UNAVAILABLE_MESSAGE, "vertex_workload_identity_unavailable")


def ensure_service_account_allowed(service_account_email: str) -> None:
    """Refuse a service account whose home project is not in
    ``GCP_WORKLOAD_IDENTITY_ALLOWED_PROJECTS`` (a comma list; unset allows every project)."""
    allowed = {
        project.strip()
        for project in os.environ.get(ALLOWED_PROJECTS_ENV, "").split(",")
        if project.strip()
    }
    home = service_account_email.partition("@")[2].removesuffix(_SERVICE_ACCOUNT_SUFFIX)
    if allowed and home not in allowed:
        raise refusal(
            f"Service account {service_account_email} is in a project this deployment does "
            "not allow.",
            "vertex_project_not_allowed",
        )


@contextmanager
def _stage(stage: str) -> Iterator[None]:
    try:
        yield
    except google_exceptions.GoogleAuthError as exc:
        # The innermost stage wins: a failure keeps the tag of the hop that raised it.
        if getattr(exc, STAGE_ATTRIBUTE, None) is None:
            setattr(exc, STAGE_ATTRIBUTE, stage)
        raise


class _BrokerHop(impersonated_credentials.Credentials):
    """One ``generateAccessToken`` call whose failures carry the stage that raised them."""

    _stage = BROKER_STAGE

    def refresh(self, request: Any) -> None:
        source = self._source_credentials
        if not source.valid:
            with _stage(SOURCE_STAGE):
                source.refresh(request)
        with _stage(self._stage):
            super().refresh(request)


class _ServiceAccountHop(_BrokerHop):
    _stage = SERVICE_ACCOUNT_STAGE


def _hop(cls: type, source: Credentials, target: str) -> Credentials:
    return cls(
        source,
        target_principal=target,
        target_scopes=_SCOPES,
        delegates=[],
        # Pinned: google.auth would otherwise derive the host from the universe and mTLS env.
        iam_endpoint_override=_GENERATE_ACCESS_TOKEN_URL.format(target),
    )


def _matched(value: Optional[str], pattern: re.Pattern) -> Optional[str]:
    value = (value or "").strip()
    return value if pattern.fullmatch(value) else None


def _deployment() -> tuple[str, str, Optional[str], Optional[str]]:
    """``(source, broker, audience, token_file)`` from deployment env."""
    source = os.environ.get(SOURCE_ENV, "").strip()
    if source not in (GKE_METADATA, IDENTITY_POOL):
        raise _unavailable(f"{SOURCE_ENV} is not {GKE_METADATA} or {IDENTITY_POOL}")
    broker = _matched(os.environ.get(BROKER_ENV), _SERVICE_ACCOUNT)
    if broker is None:
        raise _unavailable(f"{BROKER_ENV} is not a service account email")
    if source == GKE_METADATA:
        return source, broker, None, None
    audience = _matched(os.environ.get(AUDIENCE_ENV), _AUDIENCE)
    if audience is None:
        raise _unavailable(f"{AUDIENCE_ENV} is not a workload identity provider")
    token_file = os.environ.get(TOKEN_FILE_ENV, "").strip()
    if not token_file or not os.path.isfile(token_file):
        raise _unavailable(f"{TOKEN_FILE_ENV} names no projected token file")
    return source, broker, audience, token_file


def _source(source: str, audience: Optional[str], token_file: Optional[str]) -> Credentials:
    if source == GKE_METADATA:
        # An explicit universe skips the metadata lookup impersonation would make to read it.
        return compute_engine.Credentials(universe_domain=_UNIVERSE_DOMAIN)
    return identity_pool.Credentials.from_info(
        {
            "type": "external_account",
            "audience": audience,
            "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
            "token_url": _STS_TOKEN_URL,
            "credential_source": {"file": token_file, "format": {"type": "text"}},
            "universe_domain": _UNIVERSE_DOMAIN,
        }
    )


@functools.lru_cache(maxsize=128)
def _cached_credentials(
    source: str, broker: str, audience: Optional[str], token_file: Optional[str], target: str
) -> Credentials:
    broker_hop = _hop(_BrokerHop, _source(source, audience, token_file), broker)
    return _hop(_ServiceAccountHop, broker_hop, target)


def vertex_workload_identity_credentials(service_account_email: str) -> Credentials:
    """Credentials impersonating ``service_account_email``, cached per process; minted on use.

    Raises a ``UserError`` with ``error_reason`` ``vertex_workload_identity_unavailable`` when
    this deployment has no usable source, or ``vertex_project_not_allowed`` for a service
    account outside the allowed projects.
    """
    ensure_service_account_allowed(service_account_email)
    return _cached_credentials(*_deployment(), service_account_email)
