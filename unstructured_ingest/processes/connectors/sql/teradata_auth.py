"""The JWT a Teradata connection logs on with, and where it comes from.

``TeradataConnectionConfig`` opens a new ``teradatasql`` connection for every cursor,
so the token is read again at every logon. That read is the seam: a pasted token is
refused there once past ``exp`` instead of being sent, and a client-credentials token
is renewed there before it gets that far, so a long run or a recurring schedule does
not fail on a short-lived token.

Nothing here logs or raises a token, a secret, or an identity provider's free text.
"""

from __future__ import annotations

import base64
import binascii
import json
import math
import re
import ssl
import threading
import time
from typing import TYPE_CHECKING, Optional, Protocol
from urllib.parse import quote_plus

from unstructured_ingest.error import ProviderError, UserAuthError
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from requests import Response

TOKEN_EXPIRED_MESSAGE = (
    "The connector's JWT token has expired. Retrying cannot fix this. "
    "Supply a current token, or with client credentials check the identity "
    "provider's token lifetime, and re-run."
)

JWT_REFUSED_MESSAGE = (
    "Teradata refused the connector's JWT token. Check that the database trusts the "
    "token's issuer and maps the token to a database user that may log on with a null "
    "password, and re-run."
)

# Refresh this far ahead of `exp`, so a logon that starts just inside the deadline
# still presents a live token.
REFRESH_LEEWAY_SECONDS = 120.0
# How often a source is re-read when its token's expiry is unknown.
BLIND_REREAD_SECONDS = 60.0
# After a failed refresh, how long before the next attempt. Without it every logon
# for the rest of the token's life retries the identity provider.
FAILED_REFRESH_BACKOFF_SECONDS = 5.0
# Every logon waits on a due refresh, so the mint is bounded.
TOKEN_ENDPOINT_TIMEOUT_SECONDS = 30.0

# RFC 6749 5.2's registered error codes. Only these are echoed: the field is free
# text from the issuer.
OAUTH_ERROR_CODES = frozenset(
    {
        "invalid_request",
        "invalid_client",
        "invalid_grant",
        "unauthorized_client",
        "unsupported_grant_type",
        "invalid_scope",
    }
)


class MalformedTokenError(UserAuthError):
    """The token is not a compact JWS, so it was never sent."""


class TokenExpiredError(UserAuthError):
    """The token is past ``exp``: distinct from a credential the database refused."""


class TokenEndpointUnavailableError(ProviderError):
    """The identity provider could not be reached or answered 429/5xx. Retryable."""

    status_code = 503
    failure_category = "AUTH_PROVIDER_UNAVAILABLE"


_SEGMENT_RE = re.compile(r"[A-Za-z0-9_-]+")


def _malformed(reason: str) -> MalformedTokenError:
    return MalformedTokenError(
        f"The connector's JWT token is not a well-formed JWT ({reason}). "
        "Paste the complete token your identity provider issued."
    )


def _decode_segment(segment: str) -> object:
    # A base64url length of 4n+1 cannot encode any byte string.
    if not _SEGMENT_RE.fullmatch(segment) or len(segment) % 4 == 1:
        raise ValueError
    return json.loads(base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4)))


def parse_jwt(token: str) -> dict:
    """Return the claims of a compact JWS, or raise ``MalformedTokenError``.

    Checks shape only. Only Teradata can verify the signature; what this buys is a
    refusal before the token leaves the pod. Messages are fixed text.
    """
    if token.count(".") != 2:
        raise _malformed("expected three dot-separated segments")
    header_segment, claims_segment, signature = token.split(".")
    if not _SEGMENT_RE.fullmatch(signature) or len(signature) % 4 == 1:
        raise _malformed("the signature segment is empty or not base64url")
    try:
        header = _decode_segment(header_segment)
        claims = _decode_segment(claims_segment)
    except (ValueError, binascii.Error, RecursionError):
        raise _malformed("a segment does not decode as base64url JSON") from None
    if not isinstance(header, dict) or not isinstance(claims, dict):
        raise _malformed("a segment does not decode as base64url JSON")
    alg = header.get("alg")
    if not isinstance(alg, str) or not alg or alg.lower() == "none":
        raise _malformed("the header names no signing algorithm")
    exp = claims.get("exp")
    if exp is not None:
        if isinstance(exp, bool) or not isinstance(exp, (int, float)):
            raise _malformed("the exp claim is not a number")
        try:
            finite = math.isfinite(float(exp))
        except OverflowError:
            finite = False
        if not finite:
            # NaN compares False to everything, so it would never expire.
            raise _malformed("the exp claim is not a finite number")
    return claims


class TokenSource(Protocol):
    """Where the next token comes from.

    ``rereadable`` means going back to the origin can yield a different token.
    """

    rereadable: bool

    def get_token(self) -> str: ...

    def expires_at(self) -> Optional[float]: ...


class StaticTokenSource:
    """A token pasted by the user. Never changes for the life of the process."""

    rereadable = False

    def __init__(self, token: str):
        self._token = token
        self._expires_at: Optional[float] = None

    def get_token(self) -> str:
        exp = parse_jwt(self._token).get("exp")
        self._expires_at = float(exp) if exp is not None else None
        return self._token

    def expires_at(self) -> Optional[float]:
        return self._expires_at


class RefreshingToken:
    """The token each logon presents, renewed from its source when due.

    Nothing is read until the first logon, so a refusal happens there, where it is
    audited, and validating a connection config never calls an identity provider.
    """

    def __init__(self, source: TokenSource):
        self._source = source
        self._lock = threading.Lock()
        self._token: Optional[str] = None
        self._expires_at: Optional[float] = None
        self._issued_at = 0.0
        self._next_blind_reread = 0.0
        self._refresh_blocked_until = 0.0
        # With nothing usable held: when the next attempt may be made, and the error
        # the last one failed with, served to every caller until then.
        self._retry_expired_after = 0.0
        self._refresh_failure: Optional[tuple[type[BaseException], str]] = None
        # Set once the issuer refuses the credential; never cleared.
        self._refused: Optional[tuple[type[BaseException], str]] = None

    def _expired(self, now: float) -> bool:
        return self._token is None or (self._expires_at is not None and now >= self._expires_at)

    def _due(self, now: float) -> bool:
        if self._expired(now):
            first_read = self._token is None
            return (first_read or self._source.rereadable) and now >= self._retry_expired_after
        if not self._source.rereadable or now < self._refresh_blocked_until:
            return False
        if self._expires_at is not None:
            # Never earlier than half the token's life: a token shorter-lived than
            # the leeway would otherwise be due the moment it was minted.
            half_life = max(self._expires_at - self._issued_at, 0.0) / 2
            return now >= self._expires_at - min(REFRESH_LEEWAY_SECONDS, half_life)
        return now >= self._next_blind_reread

    @property
    def value(self) -> str:
        # Cheap check outside the lock; concurrent logons make one refresh between them.
        if self._refused is not None:
            raise _rebuilt(self._refused)
        if self._due(time.time()):
            with self._lock:
                if self._refused is not None:
                    raise _rebuilt(self._refused)
                now = time.time()
                if self._due(now):
                    self._refresh(now)
        now = time.time()
        if self._refused is not None:
            raise _rebuilt(self._refused)
        if self._expired(now):
            if self._refresh_failure is not None:
                raise _rebuilt(self._refresh_failure)
            # RFC 7519 4.1.4: a token past `exp` must not be accepted, so it is not
            # sent either.
            raise TokenExpiredError(TOKEN_EXPIRED_MESSAGE)
        return self._token

    def _refresh(self, now: float) -> None:
        try:
            token = self._source.get_token()
            expires_at = self._source.expires_at()
            if expires_at is not None and time.time() >= expires_at:
                # Dead on arrival: a pasted token past `exp`, or an issuer whose clock
                # or `exp` is wrong. Asking again at once cannot help.
                self._back_off(now, (TokenExpiredError, TOKEN_EXPIRED_MESSAGE))
                return
            self._token = token
            self._expires_at = expires_at
            self._issued_at = time.time()
            self._refresh_blocked_until = 0.0
            self._retry_expired_after = 0.0
            self._refresh_failure = None
        except UserAuthError as error:
            # The token or the client was refused outright. Serving a held token
            # would keep a revoked identity logging on until it expires.
            self._refused = (type(error), str(error))
            raise
        except Exception as error:
            self._back_off(now, (type(error), str(error)))
            if self._expired(now):
                raise
            logger.warning(
                f"Teradata token refresh failed ({type(error).__name__}); serving the held "
                f"token, retrying in {FAILED_REFRESH_BACKOFF_SECONDS}s"
            )
        finally:
            self._next_blind_reread = time.time() + BLIND_REREAD_SECONDS

    def _back_off(self, now: float, failure: tuple[type[BaseException], str]) -> None:
        """After a failed refresh: keep serving a live held token, else serve the failure."""
        if self._expired(now):
            self._retry_expired_after = now + FAILED_REFRESH_BACKOFF_SECONDS
            self._refresh_failure = failure
        else:
            self._refresh_blocked_until = now + FAILED_REFRESH_BACKOFF_SECONDS


def _rebuilt(failure: tuple[type[BaseException], str]) -> BaseException:
    """A fresh instance of a recorded failure, safe to raise from any thread."""
    kind, message = failure
    try:
        return kind(message)
    except Exception:
        return RuntimeError(message)


def _json_object(response: "Response") -> dict:
    try:
        body = response.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


class ClientCredentialsTokenSource:
    """Mints access tokens with an OAuth 2.0 client credentials grant (RFC 6749 4.4).

    Every ``get_token`` asks the issuer for a new token; ``RefreshingToken`` decides
    when. The client authenticates with HTTP Basic, which RFC 6749 2.3.1 requires
    every issuer to accept. The token URL is checked for https when the connection
    config is validated.
    """

    rereadable = True

    def __init__(self, token_url: str, client_id: str, client_secret: str, scope: Optional[str]):
        self._url = token_url
        pair = f"{quote_plus(client_id)}:{quote_plus(client_secret)}"
        self._authorization = "Basic " + base64.b64encode(pair.encode()).decode()
        self._scope = scope
        self._expires_at: Optional[float] = None

    @requires_dependencies(["requests"], extras="teradata")
    def get_token(self) -> str:
        import requests

        data = {"grant_type": "client_credentials"}
        if self._scope:
            data["scope"] = self._scope
        try:
            response = requests.post(
                self._url,
                data=data,
                headers={"Authorization": self._authorization, "Accept": "application/json"},
                timeout=TOKEN_ENDPOINT_TIMEOUT_SECONDS,
                allow_redirects=False,
            )
        except requests.exceptions.SSLError as error:
            if not _is_certificate_failure(error):
                raise TokenEndpointUnavailableError(
                    "Could not reach the identity provider's token endpoint (TLS failure)."
                ) from None
            # A certificate the pod does not trust is configuration, not an outage.
            raise UserAuthError(
                "The identity provider's TLS certificate could not be verified. Check the "
                "token URL, or add the provider's CA to REQUESTS_CA_BUNDLE, and re-run."
            ) from None
        except (
            requests.exceptions.InvalidURL,
            requests.exceptions.InvalidSchema,
            requests.exceptions.MissingSchema,
        ):
            raise UserAuthError(
                "The token URL is not a valid https URL. Check it and re-run."
            ) from None
        except requests.RequestException as error:
            raise TokenEndpointUnavailableError(
                f"Could not reach the identity provider's token endpoint ({type(error).__name__})."
            ) from None

        status = response.status_code
        if status in (408, 429) or status >= 500:
            raise TokenEndpointUnavailableError(
                f"The identity provider's token endpoint answered HTTP {status}."
            )
        if 300 <= status < 400:
            raise UserAuthError(
                f"The token URL redirected (HTTP {status}). Use the token endpoint's "
                "final https URL, and re-run."
            )
        body = _json_object(response)
        if not 200 <= status < 300:
            code = body.get("error")
            reason = code if isinstance(code, str) and code in OAUTH_ERROR_CODES else None
            raise UserAuthError(
                "The identity provider refused the connector's client credentials "
                f"({reason or f'HTTP {status}'}). Check the token URL, client ID, "
                "client secret and scope, and re-run."
            )
        token = body.get("access_token")
        if not isinstance(token, str) or not token:
            raise UserAuthError(
                "The identity provider's token endpoint answered without an access token. "
                "Check the token URL."
            )
        try:
            exp = parse_jwt(token).get("exp")
        except MalformedTokenError:
            raise UserAuthError(
                "The identity provider's access token is not a JWT. Configure it to issue "
                "JWT access tokens for this client, and re-run."
            ) from None
        self._expires_at = float(exp) if exp is not None else _expiry_from(body)
        logger.info("Obtained a new JWT from the identity provider's token endpoint")
        return token

    def expires_at(self) -> Optional[float]:
        return self._expires_at


def _is_certificate_failure(error: BaseException) -> bool:
    """True if a TLS failure was the certificate check, found anywhere under ``error``.

    requests raises the same ``SSLError`` for an untrusted certificate and for a
    handshake cut short; the ssl module's own error, nested in urllib3's, tells them apart.
    """
    seen: set[int] = set()
    stack: list[object] = [error]
    while stack:
        current = stack.pop()
        if not isinstance(current, BaseException) or id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, ssl.SSLCertVerificationError):
            return True
        stack.extend((current.__cause__, current.__context__, getattr(current, "reason", None)))
        stack.extend(current.args)
    return False


def _expiry_from(body: dict) -> Optional[float]:
    expires_in = body.get("expires_in")
    if isinstance(expires_in, bool):
        return None
    try:
        seconds = float(expires_in)
    except (TypeError, ValueError):
        return None
    return time.time() + seconds if math.isfinite(seconds) and seconds > 0 else None
