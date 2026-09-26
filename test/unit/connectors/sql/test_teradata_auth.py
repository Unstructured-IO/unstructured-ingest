"""The JWT a Teradata connection logs on with: its shape check, its expiry, and its renewal.

The connector opens a new connection for every cursor, so the token is read again
at every logon. That read is where a pasted token is refused once past ``exp`` and
where a client-credentials token is renewed before it gets there. The network
boundary (``requests.post``) is the only stand-in.
"""

import base64
import json
import logging
import ssl
import threading
import time

import pytest
import requests
import urllib3

from unstructured_ingest.error import ProviderError, UserAuthError
from unstructured_ingest.processes.connectors.sql import teradata_auth
from unstructured_ingest.processes.connectors.sql.teradata_auth import (
    REFRESH_LEEWAY_SECONDS,
    TOKEN_EXPIRED_MESSAGE,
    ClientCredentialsTokenSource,
    MalformedTokenError,
    RefreshingToken,
    StaticTokenSource,
    TokenEndpointUnavailableError,
    TokenExpiredError,
    parse_jwt,
)

TOKEN_URL = "https://idp.example.com/oauth2/default/v1/token"
CLIENT_ID = "svc client"
CLIENT_SECRET = "S3CR3T/with+chars"


def _seg(payload) -> str:
    return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")


def _jwt(exp_delta, **claims) -> str:
    payload = dict(claims)
    if exp_delta is not None:
        payload["exp"] = int(time.time()) + exp_delta
    return f"{_seg({'alg': 'RS256'})}.{_seg(payload)}.c2lnbmF0dXJl"


def _response(status: int, body) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    if isinstance(body, (dict, list)):
        response._content = json.dumps(body).encode()
        response.headers["Content-Type"] = "application/json"
    else:
        response._content = (body or "").encode()
        response.headers["Content-Type"] = "text/html"
    return response


class _Endpoint:
    """Records every POST and answers from a script, one answer per call."""

    def __init__(self, *answers):
        self.answers = list(answers)
        self.calls = []

    def __call__(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        answer = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        if isinstance(answer, Exception):
            raise answer
        return answer


@pytest.fixture
def endpoint(monkeypatch):
    def install(*answers):
        ep = _Endpoint(*answers)
        monkeypatch.setattr(requests, "post", ep)
        return ep

    return install


def _source(scope=None):
    return ClientCredentialsTokenSource(TOKEN_URL, CLIENT_ID, CLIENT_SECRET, scope=scope)


def _tls_error(cause: BaseException) -> requests.exceptions.SSLError:
    """What requests raises for a TLS failure: the ssl error sits under urllib3's."""
    reason = urllib3.exceptions.SSLError(cause)
    return requests.exceptions.SSLError(
        urllib3.exceptions.MaxRetryError(None, TOKEN_URL, reason=reason)
    )


class _Counting:
    """Mints a fresh, hour-long token on each read."""

    rereadable = True

    def __init__(self, lifetime=3600.0):
        self.lifetime = lifetime
        self.calls = 0

    def get_token(self):
        self.calls += 1
        return f"TOKEN-{self.calls}"

    def expires_at(self):
        return time.time() + self.lifetime


def _due_but_valid(token: RefreshingToken) -> None:
    """Put the held token inside the refresh window, still in the future."""
    token._issued_at = time.time() - 3600
    token._expires_at = time.time() + REFRESH_LEEWAY_SECONDS / 2
    token._refresh_blocked_until = 0


class TestParseJwt:
    def test_reads_claims_without_verifying_the_signature(self):
        # Only Teradata can verify the signature; the connector checks shape.
        assert parse_jwt(_jwt(3600, sub="svc"))["sub"] == "svc"

    @pytest.mark.parametrize(
        "token",
        [
            "",
            "not-a-jwt",
            "a.b.c",
            "a.b",
            f"{_seg({'alg': 'RS256'})}.{_seg({'exp': 1})}.sig.more.parts",
            f"{_seg({'alg': 'RS256'})}.{_seg([1, 2])}.sig",
            f"{_seg(['RS256'])}.{_seg({'exp': 1})}.sig",
            f"{_seg({'typ': 'JWT'})}.{_seg({'exp': 1})}.sig",
            f"{_seg({'alg': 'RS256'})}.{_seg({'exp': 'tomorrow'})}.sig",
            f"{_seg({'alg': 'RS256'})}.{_seg({'exp': True})}.sig",
            # NaN compares False to everything, so the token would never expire.
            f"{_seg({'alg': 'RS256'})}.{_seg({'exp': float('nan')})}.sig",
            f"{_seg({'alg': 'RS256'})}.{_seg({'exp': float('inf')})}.sig",
            f"{_seg({'alg': 'RS256'})}.{_seg({'exp': 10**400})}.sig",
            f"{_seg({'alg': 'none'})}.{_seg({'exp': 1})}.",
            f"{_seg({'alg': 'NONE'})}.{_seg({'exp': 1})}.sig",
            f"{_seg({'alg': 'RS256'})}.{_seg({'exp': 1})}.",
            f"{_seg({'alg': 'RS256'})}.{_seg({'exp': 1})}.!!!@@@",
            # Five base64url characters cannot encode any byte string.
            f"{_seg({'alg': 'RS256'})}.{_seg({'exp': 1})}.abcde",
            f"{_seg({'alg': 'RS256'})}.{_seg({'exp': 1})}.a",
            f"{_seg({'alg': 123})}.{_seg({'exp': 1})}.sig",
            # A segment outside the base64url alphabet must not be decoded leniently.
            f"{_seg({'alg': 'RS256'})}.{_seg({'sub': 'x'})}!.sig",
            f"{_seg({'alg': 'RS256'})}.bm90IGpzb24.sig",
        ],
    )
    def test_a_structurally_invalid_token_is_refused_before_use(self, token):
        with pytest.raises(MalformedTokenError):
            parse_jwt(token)

    def test_a_malformed_token_is_an_authentication_error(self):
        # AC 6.3: a malformed token fails as an auth error, not a config one.
        assert issubclass(MalformedTokenError, UserAuthError)

    def test_the_refusal_never_echoes_the_token(self):
        secret_segment = _seg({"sub": "LEAKED-SUBJECT"})
        for token in (
            f"{secret_segment}.{secret_segment}",
            f"{secret_segment}.{secret_segment}.c2ln",
        ):
            with pytest.raises(MalformedTokenError) as exc:
                parse_jwt(token)
            assert secret_segment not in str(exc.value)
            assert "LEAKED-SUBJECT" not in str(exc.value)

    def test_a_token_with_no_exp_claim_is_well_formed(self):
        assert "exp" not in parse_jwt(_jwt(None, sub="svc"))


class TestAPastedToken:
    def test_it_is_served_unchanged(self):
        token = _jwt(3600)
        assert RefreshingToken(StaticTokenSource(token)).value == token

    def test_nothing_is_parsed_until_the_first_logon(self):
        # Construction happens while the connection config is validated; the
        # refusal belongs to the logon, where it is audited.
        holder = RefreshingToken(StaticTokenSource("not-a-jwt"))
        with pytest.raises(MalformedTokenError):
            holder.value

    def test_a_malformed_token_stays_refused(self):
        holder = RefreshingToken(StaticTokenSource("not-a-jwt"))
        for _ in range(2):
            with pytest.raises(MalformedTokenError):
                holder.value

    def test_an_expired_token_is_refused_before_it_is_sent(self):
        with pytest.raises(TokenExpiredError) as exc:
            RefreshingToken(StaticTokenSource(_jwt(-60))).value
        assert str(exc.value) == TOKEN_EXPIRED_MESSAGE

    def test_a_token_that_expires_mid_run_is_refused_at_the_next_logon(self):
        holder = RefreshingToken(StaticTokenSource(_jwt(3600)))
        holder.value
        holder._expires_at = time.time() - 1
        with pytest.raises(TokenExpiredError):
            holder.value

    def test_expired_is_distinct_from_a_generic_rejection(self):
        # FR-4: an operator can tell expiry from a bad credential.
        assert issubclass(TokenExpiredError, UserAuthError)
        assert TokenExpiredError is not UserAuthError
        assert "expired" in TOKEN_EXPIRED_MESSAGE

    def test_the_expiry_error_carries_no_token_content(self):
        token = _jwt(-60, sub="LEAKED-SUBJECT")
        with pytest.raises(TokenExpiredError) as exc:
            RefreshingToken(StaticTokenSource(token)).value
        assert "LEAKED-SUBJECT" not in str(exc.value)
        assert token.split(".")[1] not in str(exc.value)

    def test_a_token_with_no_exp_is_served_indefinitely(self):
        token = _jwt(None, sub="svc")
        assert RefreshingToken(StaticTokenSource(token)).value == token


class TestRenewal:
    def test_the_first_logon_mints(self):
        source = _Counting()
        holder = RefreshingToken(source)
        assert source.calls == 0
        assert holder.value == "TOKEN-1"

    def test_a_fresh_token_is_not_reminted_until_due(self):
        source = _Counting()
        holder = RefreshingToken(source)
        for _ in range(10):
            holder.value
        assert source.calls == 1

    def test_a_token_inside_the_refresh_window_is_replaced(self):
        source = _Counting()
        holder = RefreshingToken(source)
        holder.value
        _due_but_valid(holder)
        assert holder.value == "TOKEN-2"

    def test_a_token_shorter_lived_than_the_leeway_is_not_reminted_every_logon(self):
        source = _Counting(lifetime=60)
        holder = RefreshingToken(source)
        for _ in range(20):
            holder.value
        assert source.calls == 1

    def test_a_short_lived_token_is_still_renewed_past_its_midpoint(self):
        source = _Counting(lifetime=60)
        holder = RefreshingToken(source)
        holder.value
        holder._issued_at -= 31
        holder._expires_at = holder._issued_at + 60
        assert holder.value == "TOKEN-2"

    def test_concurrent_logons_collapse_into_one_mint(self):
        source = _Counting()
        holder = RefreshingToken(source)
        holder.value
        holder._expires_at = time.time() - 1
        seen = []
        threads = [threading.Thread(target=lambda: seen.append(holder.value)) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert source.calls == 2
        assert set(seen) == {"TOKEN-2"}


class _FailsAfterFirst:
    rereadable = True

    def __init__(self, error):
        self.error = error
        self.calls = 0

    def get_token(self):
        self.calls += 1
        if self.calls == 1:
            return "GOOD"
        raise self.error

    def expires_at(self):
        return time.time() + 3600


class TestRenewalFailure:
    def test_an_outage_is_ridden_out_on_the_held_token_and_backs_off(self):
        source = _FailsAfterFirst(TokenEndpointUnavailableError("down"))
        holder = RefreshingToken(source)
        holder.value
        _due_but_valid(holder)
        for _ in range(5):
            assert holder.value == "GOOD"
        # One failed attempt; the rest are inside the backoff window.
        assert source.calls == 2

    def test_an_expired_token_raises_the_outage_instead_of_sending_a_dead_credential(self):
        source = _FailsAfterFirst(TokenEndpointUnavailableError("down"))
        holder = RefreshingToken(source)
        holder.value
        holder._expires_at = time.time() - 1
        for _ in range(3):
            with pytest.raises(TokenEndpointUnavailableError):
                holder.value
        # Callers inside the backoff get the recorded verdict, not a new attempt each.
        assert source.calls == 2

    def test_a_refusal_by_the_issuer_is_not_masked_by_the_held_token(self):
        # NFR-4: a revoked client must not keep logging on until the held token expires.
        source = _FailsAfterFirst(UserAuthError("client revoked"))
        holder = RefreshingToken(source)
        holder.value
        _due_but_valid(holder)
        for _ in range(3):
            with pytest.raises(UserAuthError, match="client revoked"):
                holder.value
        # Refused once; the issuer is not asked again.
        assert source.calls == 2

    def test_a_token_expired_on_arrival_backs_off_instead_of_reminting_every_logon(self):
        # Pod clock ahead of the issuer, or a bad `exp`: every mint is already dead.
        class DeadOnArrival:
            rereadable = True
            calls = 0

            def get_token(self):
                self.calls += 1
                return "DEAD"

            def expires_at(self):
                return time.time() - 10

        source = DeadOnArrival()
        holder = RefreshingToken(source)
        for _ in range(10):
            with pytest.raises(TokenExpiredError) as exc:
                holder.value
            assert str(exc.value) == TOKEN_EXPIRED_MESSAGE
        assert source.calls == 1
        holder._retry_expired_after = 0
        with pytest.raises(TokenExpiredError):
            holder.value
        assert source.calls == 2

    def test_a_dead_replacement_for_a_live_token_backs_off_and_keeps_the_held_one(self):
        class DeadReplacement:
            rereadable = True
            calls = 0

            def get_token(self):
                self.calls += 1
                return "GOOD" if self.calls == 1 else "DEAD"

            def expires_at(self):
                return time.time() + 3600 if self.calls == 1 else time.time() - 10

        source = DeadReplacement()
        holder = RefreshingToken(source)
        holder.value
        _due_but_valid(holder)
        for _ in range(10):
            assert holder.value == "GOOD"
        assert source.calls == 2

    def test_an_outage_at_the_first_logon_is_raised(self):
        class Down:
            rereadable = True

            def get_token(self):
                raise TokenEndpointUnavailableError("down")

            def expires_at(self):
                return None

        with pytest.raises(TokenEndpointUnavailableError):
            RefreshingToken(Down()).value


class TestTheClientCredentialsRequest:
    def test_it_asks_for_a_client_credentials_grant_with_basic_client_auth(self, endpoint):
        ep = endpoint(_response(200, {"access_token": _jwt(3600), "expires_in": 3600}))
        _source().get_token()
        call = ep.calls[0]
        assert call["url"] == TOKEN_URL
        assert call["data"]["grant_type"] == "client_credentials"
        # RFC 6749 2.3.1: id and secret are form-urlencoded before Basic encoding.
        expected = base64.b64encode(b"svc+client:S3CR3T%2Fwith%2Bchars").decode()
        assert call["headers"]["Authorization"] == f"Basic {expected}"

    def test_the_scope_is_sent_only_when_configured(self, endpoint):
        ep = endpoint(_response(200, {"access_token": _jwt(3600)}))
        _source().get_token()
        _source(scope="teradata.logon").get_token()
        assert "scope" not in ep.calls[0]["data"]
        assert ep.calls[1]["data"]["scope"] == "teradata.logon"

    def test_the_call_is_bounded_and_does_not_follow_redirects(self, endpoint):
        # A redirect would carry the client secret to a host nobody configured.
        ep = endpoint(_response(200, {"access_token": _jwt(3600)}))
        _source().get_token()
        assert ep.calls[0]["timeout"] > 0
        assert ep.calls[0]["allow_redirects"] is False


class TestTheClientCredentialsAnswer:
    def test_every_read_mints_a_new_token(self, endpoint):
        first, second = _jwt(3600, n=1), _jwt(3600, n=2)
        endpoint(
            _response(200, {"access_token": first}),
            _response(200, {"access_token": second}),
        )
        source = _source()
        assert source.rereadable is True
        assert (source.get_token(), source.get_token()) == (first, second)

    def test_expiry_comes_from_the_token(self, endpoint):
        endpoint(_response(200, {"access_token": _jwt(600), "expires_in": 99999}))
        source = _source()
        source.get_token()
        assert source.expires_at() == pytest.approx(time.time() + 600, abs=5)

    def test_expires_in_is_the_fallback_when_the_token_has_no_exp(self, endpoint):
        endpoint(_response(200, {"access_token": _jwt(None), "expires_in": 900}))
        source = _source()
        source.get_token()
        assert source.expires_at() == pytest.approx(time.time() + 900, abs=5)

    def test_an_access_token_that_is_not_a_jwt_is_refused(self, endpoint):
        # logmech=JWT takes a JWT; an opaque token would only fail at logon. Nobody
        # pasted it, so the message points at the identity provider.
        endpoint(_response(200, {"access_token": "opaque-reference-token"}))
        with pytest.raises(UserAuthError) as exc:
            _source().get_token()
        assert "issue JWT access tokens" in str(exc.value)
        assert "Paste" not in str(exc.value)
        assert "opaque-reference-token" not in str(exc.value)

    def test_an_answer_without_an_access_token_is_an_auth_error(self, endpoint):
        endpoint(_response(200, "<html>login</html>"))
        with pytest.raises(UserAuthError, match="access token"):
            _source().get_token()


class TestClientCredentialsRefusalsAndOutages:
    @pytest.mark.parametrize("status", [400, 401, 403])
    def test_a_refusal_is_a_terminal_auth_error_naming_the_oauth_error_code(self, endpoint, status):
        endpoint(
            _response(
                status,
                {"error": "invalid_client", "error_description": f"bad secret {CLIENT_SECRET}"},
            )
        )
        with pytest.raises(UserAuthError) as exc:
            _source().get_token()
        assert "invalid_client" in str(exc.value)
        assert CLIENT_SECRET not in str(exc.value)

    @pytest.mark.parametrize(
        "code", ["see https://x/?secret=abc and more words", "s3cr3t_value", "client_secret"]
    )
    def test_only_a_registered_oauth_error_code_is_echoed(self, endpoint, code):
        endpoint(_response(400, {"error": code}))
        with pytest.raises(UserAuthError) as exc:
            _source().get_token()
        assert code not in str(exc.value)
        assert "(HTTP 400)" in str(exc.value)

    @pytest.mark.parametrize("code", [["invalid_client"], {"x": 1}, 7, None])
    def test_a_non_string_error_code_is_still_a_handled_refusal(self, endpoint, code):
        endpoint(_response(401, {"error": code}))
        with pytest.raises(UserAuthError, match="HTTP 401"):
            _source().get_token()

    def test_a_redirect_is_a_misconfigured_token_url(self, endpoint):
        endpoint(_response(302, ""))
        with pytest.raises(UserAuthError, match="token URL redirected"):
            _source().get_token()

    @pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504])
    def test_a_server_side_failure_is_a_retryable_outage(self, endpoint, status):
        endpoint(_response(status, {"error": "temporarily_unavailable"}))
        with pytest.raises(TokenEndpointUnavailableError) as exc:
            _source().get_token()
        # 503 without the terminality marker is what the controller re-dispatches.
        assert isinstance(exc.value, ProviderError)
        assert exc.value.status_code == 503
        assert exc.value.failure_category == "AUTH_PROVIDER_UNAVAILABLE"

    @pytest.mark.parametrize("error", [requests.ConnectionError, requests.Timeout])
    def test_a_connection_failure_is_a_retryable_outage_that_names_no_secret(self, endpoint, error):
        endpoint(error(f"refused for {CLIENT_SECRET}"))
        with pytest.raises(TokenEndpointUnavailableError) as exc:
            _source().get_token()
        assert CLIENT_SECRET not in str(exc.value)
        assert exc.value.__suppress_context__

    @pytest.mark.parametrize(
        "error, words",
        [
            # A certificate the pod does not trust is configuration, not an outage.
            (
                _tls_error(ssl.SSLCertVerificationError(1, "certificate verify failed")),
                "certificate",
            ),
            (requests.exceptions.InvalidURL(f"bad for {CLIENT_SECRET}"), "token URL"),
        ],
    )
    def test_a_permanent_endpoint_problem_is_a_terminal_auth_error(self, endpoint, error, words):
        endpoint(error)
        with pytest.raises(UserAuthError, match=words) as exc:
            _source().get_token()
        assert CLIENT_SECRET not in str(exc.value)

    @pytest.mark.parametrize(
        "cause",
        [ssl.SSLEOFError(8, "EOF occurred in violation of protocol"), ssl.SSLError(1, "handshake")],
    )
    def test_a_tls_failure_that_is_not_the_certificate_is_a_retryable_outage(self, endpoint, cause):
        endpoint(_tls_error(cause))
        with pytest.raises(TokenEndpointUnavailableError):
            _source().get_token()

    def test_a_tls_blip_is_ridden_out_on_the_held_token(self, endpoint):
        held = _jwt(3600, n=1)
        endpoint(
            _response(200, {"access_token": held}),
            _tls_error(ssl.SSLEOFError(8, "EOF occurred in violation of protocol")),
        )
        holder = RefreshingToken(_source())
        holder.value
        _due_but_valid(holder)
        assert holder.value == held

    def test_nothing_secret_is_logged(self, endpoint, caplog):
        token = _jwt(3600)
        endpoint(
            _response(200, {"access_token": token}),
            _response(401, {"error": "invalid_client", "error_description": CLIENT_SECRET}),
        )
        with caplog.at_level(logging.DEBUG):
            source = _source()
            source.get_token()
            with pytest.raises(UserAuthError):
                source.get_token()
        assert CLIENT_SECRET not in caplog.text
        assert token.split(".")[1] not in caplog.text


class TestUnderTheHolder:
    def test_a_long_run_logs_on_with_a_fresh_token_before_the_held_one_expires(self, endpoint):
        # FR-5: the held token is inside the refresh window, so the next logon
        # mints and presents a new one instead of failing at `exp`.
        first, second = _jwt(60, n=1), _jwt(3600, n=2)
        endpoint(
            _response(200, {"access_token": first}),
            _response(200, {"access_token": second}),
        )
        holder = RefreshingToken(_source())
        assert holder.value == first
        holder._issued_at -= 3600
        assert holder.value == second

    def test_the_holder_is_the_only_caller_of_the_endpoint(self, endpoint):
        ep = endpoint(_response(200, {"access_token": _jwt(3600)}))
        holder = RefreshingToken(_source())
        for _ in range(5):
            holder.value
        assert len(ep.calls) == 1


def test_requests_is_imported_only_when_a_token_is_minted():
    # The stager imports this module and never mints; nothing here may need a
    # package at import time that only the client-credentials path uses.
    assert "requests" not in vars(teradata_auth)
