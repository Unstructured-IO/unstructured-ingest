"""JWT authentication for the Teradata SQL connector, source and destination.

What reaches ``teradatasql.connect`` is the contract with the database: password
logon must be byte-for-byte what it was (FR-7), and JWT logon is ``logmech=JWT``
with ``logdata=token=<JWT>`` and no user or password (the driver README's JWT row;
the database user is the one the token maps to). ``connect`` is replaced by a
recorder, as the rest of this suite does.
"""

import base64
import json
import logging
import time
import traceback
from unittest.mock import MagicMock

import pytest
import requests
from pydantic import Secret, ValidationError
from pytest_mock import MockerFixture

from unstructured_ingest.error import (
    ConnectionError as IngestConnectionError,
)
from unstructured_ingest.error import (
    DestinationConnectionError,
    SourceConnectionError,
    UserAuthError,
)
from unstructured_ingest.processes.connectors.sql.teradata import (
    TeradataAccessConfig,
    TeradataConnectionConfig,
    TeradataDownloader,
    TeradataDownloaderConfig,
    TeradataIndexer,
    TeradataIndexerConfig,
    TeradataUploader,
    TeradataUploaderConfig,
)
from unstructured_ingest.processes.connectors.sql.teradata_auth import (
    JWT_REFUSED_MESSAGE,
    TOKEN_EXPIRED_MESSAGE,
    MalformedTokenError,
    TokenEndpointUnavailableError,
    TokenExpiredError,
)

HOST = "td.example.com"
TOKEN_URL = "https://idp.example.com/oauth2/default/v1/token"
PASSWORD = "PW-SECRET-9a"
CLIENT_SECRET = "CS-SECRET-7b"


def _seg(payload) -> str:
    return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")


def _jwt(exp_delta=3600, sub="svc") -> str:
    payload = {"sub": sub}
    if exp_delta is not None:
        payload["exp"] = int(time.time()) + exp_delta
    return f"{_seg({'alg': 'RS256'})}.{_seg(payload)}.c2lnbmF0dXJl"


class _FakeTeradataDriverError(Exception):
    """Stand-in for a teradatasql driver exception (same module signature)."""


_FakeTeradataDriverError.__module__ = "teradatasql"


def _config(**kwargs) -> TeradataConnectionConfig:
    access = {k: kwargs.pop(k) for k in ("password", "token", "client_secret") if k in kwargs}
    return TeradataConnectionConfig(
        host=HOST,
        database="db",
        access_config=Secret(TeradataAccessConfig(**access)),
        **kwargs,
    )


def _password_config(**kwargs):
    return _config(user="u1", password=PASSWORD, **kwargs)


def _client_credentials_config(**kwargs):
    return _config(token_url=TOKEN_URL, client_id="svc", client_secret=CLIENT_SECRET, **kwargs)


@pytest.fixture
def driver(mocker: MockerFixture, monkeypatch):
    """Record every ``teradatasql.connect`` call; the proxy env stays empty."""
    for var in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "NO_PROXY", "no_proxy"):
        monkeypatch.delenv(var, raising=False)
    module = MagicMock()
    module.connect.return_value = MagicMock()
    mocker.patch.dict("sys.modules", {"teradatasql": module})
    return module


def _logon(config: TeradataConnectionConfig) -> None:
    with config.get_connection():
        pass


def _connect_kwargs(driver) -> list[dict]:
    return [c.kwargs for c in driver.connect.call_args_list]


# --- FR-1: choosing a method -------------------------------------------------


class TestChoosingAMethod:
    def test_a_password_config_is_still_accepted(self):
        assert _password_config().auth_method == "password"

    def test_a_pasted_jwt_needs_no_user_or_password(self):
        assert _config(token=_jwt()).auth_method == "jwt"

    def test_client_credentials_select_jwt_via_client_credentials(self):
        assert _client_credentials_config(scope="x").auth_method == "jwt_client_credentials"

    @pytest.mark.parametrize(
        "kwargs, words",
        [
            ({}, "Authentication required"),
            ({"user": "u1"}, "Authentication required"),
            ({"user": "u1", "password": PASSWORD, "token": "t"}, "Provide one credential set"),
            (
                {"token": "t", "token_url": TOKEN_URL, "client_id": "c", "client_secret": "s"},
                "Provide one credential set",
            ),
            ({"token_url": TOKEN_URL, "client_id": "c"}, "client secret"),
            ({"client_id": "c", "client_secret": "s"}, "token URL"),
            ({"token_url": TOKEN_URL, "client_secret": "s"}, "client ID"),
            ({"password": PASSWORD}, "username"),
            (
                {"token_url": "http://idp.example.com/t", "client_id": "c", "client_secret": "s"},
                "token URL must start with https://",
            ),
        ],
    )
    def test_exactly_one_complete_credential_set_is_accepted(self, kwargs, words):
        with pytest.raises(ValidationError, match=words):
            _config(**kwargs)

    def test_scope_alone_selects_nothing(self):
        with pytest.raises(ValidationError, match="Authentication required"):
            _config(scope="x")

    @pytest.mark.parametrize("blank", ["", "   "])
    def test_a_blank_new_credential_counts_as_absent(self, blank):
        config = _password_config(token_url=blank, client_id=blank, scope=blank)
        assert config.auth_method == "password"
        assert _config(user="u1", password=PASSWORD, token=blank).auth_method == "password"

    def test_an_empty_password_is_still_a_password(self):
        # FR-7: a stored config with password "" behaved as a password config
        # before JWT existed; it must not start failing validation now.
        assert _config(user="u1", password="").auth_method == "password"

    def test_the_messages_quote_no_value(self):
        for kwargs in (
            {"user": "u1", "password": PASSWORD, "token": _jwt()},
            {
                "token_url": "http://idp.example.com/t",
                "client_id": "c",
                "client_secret": CLIENT_SECRET,
            },
            {"token_url": TOKEN_URL, "client_id": "c-ID-VALUE"},
        ):
            with pytest.raises(ValidationError) as exc:
                _config(**kwargs)
            messages = " ".join(e["msg"] for e in exc.value.errors(include_input=False))
            for value in (PASSWORD, CLIENT_SECRET, _jwt().split(".")[1], "c-ID-VALUE"):
                assert value not in messages

    def test_secrets_are_masked_in_repr_and_dumps(self):
        token = _jwt()
        for config in (_config(token=token), _client_credentials_config()):
            rendered = repr(config) + config.model_dump_json() + str(config.model_dump())
            assert token not in rendered
            assert CLIENT_SECRET not in rendered

    def test_the_new_secrets_live_where_the_password_does(self):
        # FR-6: the secret store keeps every string inside access_config.
        access = TeradataAccessConfig.model_fields
        assert {"password", "token", "client_secret"} <= set(access)
        assert not {"token_url", "client_id", "scope"} & set(access)


# --- FR-2 / FR-7: what the driver is given ------------------------------------


class TestWhatTheDriverIsGiven:
    def test_password_logon_is_unchanged(self, driver):
        _logon(_password_config())
        # In the same order too: the driver serializes these kwargs as they come.
        assert [list(kwargs.items()) for kwargs in _connect_kwargs(driver)] == [
            [
                ("host", HOST),
                ("user", "u1"),
                ("password", PASSWORD),
                ("dbs_port", 1025),
                ("database", "db"),
            ]
        ]

    def test_a_pasted_jwt_logs_on_with_logmech_jwt(self, driver):
        token = _jwt()
        _logon(_config(token=token))
        assert _connect_kwargs(driver) == [
            {
                "host": HOST,
                "logmech": "JWT",
                "logdata": f"token={token}",
                "dbs_port": 1025,
                "database": "db",
            }
        ]

    def test_a_user_given_with_a_jwt_is_not_sent(self, driver):
        # The database user comes from the token; a second identity would conflict.
        _logon(_config(user="u1", token=_jwt()))
        assert "user" not in _connect_kwargs(driver)[0]

    def test_proxy_settings_still_apply(self, driver, monkeypatch):
        monkeypatch.setenv("HTTPS_PROXY", "http://proxy:3128")
        _logon(_config(token=_jwt()))
        assert _connect_kwargs(driver)[0]["https_proxy"] == "http://proxy:3128"

    def test_client_credentials_log_on_with_the_minted_token(self, driver, monkeypatch):
        minted = [_jwt(sub="first"), _jwt(sub="second")]
        answers = iter(minted)
        monkeypatch.setattr(
            requests, "post", lambda *a, **k: _token_response({"access_token": next(answers)})
        )
        config = _client_credentials_config()
        _logon(config)
        _logon(config)
        logdata = [kwargs["logdata"] for kwargs in _connect_kwargs(driver)]
        # One mint serves every logon until the token nears expiry.
        assert logdata == [f"token={minted[0]}", f"token={minted[0]}"]
        config._token._issued_at -= 7200
        config._token._expires_at = time.time() + 1
        _logon(config)
        assert _connect_kwargs(driver)[-1]["logdata"] == f"token={minted[1]}"


def _token_response(body, status=200) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(body).encode()
    return response


# --- FR-3 / FR-4: refused before anything is sent -----------------------------


class TestRefusedBeforeLogon:
    def test_a_malformed_token_never_reaches_the_driver(self, driver):
        with pytest.raises(MalformedTokenError):
            _logon(_config(token="not.a.jwt"))
        driver.connect.assert_not_called()

    def test_an_expired_token_never_reaches_the_driver(self, driver):
        with pytest.raises(TokenExpiredError) as exc:
            _logon(_config(token=_jwt(-60)))
        assert str(exc.value) == TOKEN_EXPIRED_MESSAGE
        driver.connect.assert_not_called()

    def test_an_identity_provider_outage_never_reaches_the_driver(self, driver, monkeypatch):
        def down(*args, **kwargs):
            raise requests.ConnectionError("down")

        monkeypatch.setattr(requests, "post", down)
        with pytest.raises(TokenEndpointUnavailableError):
            _logon(_client_credentials_config())
        driver.connect.assert_not_called()


# --- The database's own verdict on a JWT ---------------------------------------

_EXPIRED_TEXT = (
    "[Version 20.0.0.66] [Session 0] [Teradata SQL Driver] [Error 8017] "
    "JWT Token expired password=LEAKY"
)
_REJECTED_TEXT = (
    "[Version 20.0.0.66] [Session 0] [Teradata SQL Driver] An invalid JWT token is passed "
    "password=LEAKY"
)
_UNREACHABLE_TEXT = (
    "[Version 20.0.0.66] [Session 0] [Teradata SQL Driver] [Error 444] Failed to connect to "
    f"{HOST} Caused by dial tcp: connect: connection refused password=LEAKY"
)


class TestTheDatabaseVerdictOnAJwt:
    @pytest.mark.parametrize(
        "text, error_type, message",
        [
            (_EXPIRED_TEXT, TokenExpiredError, TOKEN_EXPIRED_MESSAGE),
            (_REJECTED_TEXT, UserAuthError, JWT_REFUSED_MESSAGE),
            (
                _UNREACHABLE_TEXT,
                IngestConnectionError,
                f"Failed to connect to server {HOST}: connection refused",
            ),
        ],
    )
    def test_a_logon_failure_becomes_fixed_text(self, driver, text, error_type, message):
        driver.connect.side_effect = _FakeTeradataDriverError(text)
        with pytest.raises(error_type) as exc:
            _logon(_config(token=_jwt()))
        assert str(exc.value) == message
        rendered = "".join(traceback.format_exception(exc.value))
        assert "LEAKY" not in rendered

    @pytest.mark.parametrize(
        "text",
        [
            # Not about the token; the driver's LogonController stack frames must not
            # turn it into a refusal.
            "[Teradata Database] [Error 8024] All virtual circuits are currently in use.\n"
            "  at gosqldriver/teradatasql.(*teradataConnection).logonController "
            "LogonController.go:112",
        ],
    )
    def test_a_logon_failure_that_is_not_about_the_token_is_not_called_a_refusal(
        self, driver, text
    ):
        driver.connect.side_effect = _FakeTeradataDriverError(text)
        with pytest.raises(IngestConnectionError):
            _logon(_config(token=_jwt()))

    def test_a_driver_error_numbered_8017_is_not_the_database_refusing(self, driver):
        driver.connect.side_effect = _FakeTeradataDriverError(
            "[Version 20.0.0.66] [Session 0] [Teradata SQL Driver] [Error 8017] something local"
        )
        with pytest.raises(IngestConnectionError):
            _logon(_config(token=_jwt()))

    def test_the_database_refusing_the_logon_is_a_refusal(self, driver):
        driver.connect.side_effect = _FakeTeradataDriverError(
            "[Teradata Database] [Error 8017] The UserId, Password or Account is invalid."
        )
        with pytest.raises(UserAuthError) as exc:
            _logon(_config(token=_jwt()))
        assert str(exc.value) == JWT_REFUSED_MESSAGE

    def test_a_password_logon_failure_is_raised_as_before(self, driver):
        # FR-7: the password path keeps its classification at every call site.
        error = _FakeTeradataDriverError(_UNREACHABLE_TEXT)
        driver.connect.side_effect = error
        with pytest.raises(_FakeTeradataDriverError) as exc:
            _logon(_password_config())
        assert exc.value is error


# --- Precheck: the verdict reaches the platform intact --------------------------


def _indexer(config):
    return TeradataIndexer(
        connection_config=config,
        index_config=TeradataIndexerConfig(table_name="t", id_column="id"),
    )


def _uploader(config, table_name="t"):
    return TeradataUploader(
        connection_config=config, upload_config=TeradataUploaderConfig(table_name=table_name)
    )


def _downloader(config):
    return TeradataDownloader(
        connection_config=config,
        download_config=TeradataDownloaderConfig(id_column="id"),
    )


class TestPrecheck:
    @pytest.mark.parametrize("build", [_indexer, _uploader])
    def test_an_expired_token_fails_precheck_as_expired(self, driver, build):
        with pytest.raises(TokenExpiredError):
            build(_config(token=_jwt(-60))).precheck()

    @pytest.mark.parametrize("build", [_indexer, _uploader])
    def test_a_refused_jwt_fails_precheck_as_an_auth_error(self, driver, build):
        driver.connect.side_effect = _FakeTeradataDriverError(_REJECTED_TEXT)
        with pytest.raises(UserAuthError) as exc:
            build(_config(token=_jwt())).precheck()
        assert str(exc.value) == JWT_REFUSED_MESSAGE

    @pytest.mark.parametrize(
        "build, error_type",
        [(_indexer, SourceConnectionError), (_uploader, DestinationConnectionError)],
    )
    def test_a_connection_failure_keeps_no_driver_text_in_its_traceback(
        self, driver, build, error_type
    ):
        # The SDK logs precheck failures with exc_info; the driver text must not ride along.
        driver.connect.side_effect = _FakeTeradataDriverError(_UNREACHABLE_TEXT)
        with pytest.raises(error_type) as exc:
            build(_password_config()).precheck()
        assert "LEAKY" not in "".join(traceback.format_exception(exc.value))

    def test_a_token_that_expires_during_the_create_probe_fails_precheck(self, driver):
        # The SELECT 1 logs on; the token expires before the CREATE TABLE probe logs
        # on again. That must not read as an inconclusive probe and pass.
        config = _config(token=_jwt())

        def expire_after_first_logon(**kwargs):
            config._token._expires_at = time.time() - 1
            return MagicMock()

        driver.connect.side_effect = expire_after_first_logon
        with pytest.raises(TokenExpiredError):
            _uploader(config).precheck()

    def test_the_downloader_surfaces_the_same_verdict(self, driver):
        batch = MagicMock()
        batch.additional_metadata.table_name = "t"
        batch.additional_metadata.id_column = "id"
        batch.batch_items = [MagicMock(identifier="1")]
        with pytest.raises(TokenExpiredError):
            _downloader(_config(token=_jwt(-60))).query_db(batch)


# --- FR-8: the audit line ------------------------------------------------------


@pytest.fixture
def audit(caplog):
    caplog.set_level(logging.INFO, logger="unstructured_ingest")
    return caplog


def _audit_lines(caplog) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.getMessage().startswith("Teradata authentication:")
    ]


class TestAudit:
    def test_each_method_is_recorded_with_the_run(self, driver, audit, monkeypatch):
        monkeypatch.setenv("JOB_ID", "job-1")
        monkeypatch.setenv("DAG_NODE_ID", "node-1")
        monkeypatch.setattr(
            requests, "post", lambda *a, **k: _token_response({"access_token": _jwt()})
        )
        for config in (_password_config(), _config(token=_jwt()), _client_credentials_config()):
            _logon(config)
        assert _audit_lines(audit) == [
            f"Teradata authentication: auth.method={method} outcome=authenticated "
            "job_id=job-1 dag_node_id=node-1"
            for method in ("password", "jwt", "jwt_client_credentials")
        ]

    def test_a_run_records_each_outcome_once(self, driver, audit):
        config = _config(token=_jwt())
        for _ in range(3):
            _logon(config)
        driver.connect.side_effect = _FakeTeradataDriverError(_EXPIRED_TEXT)
        for _ in range(2):
            with pytest.raises(TokenExpiredError):
                _logon(config)
        assert [line.split(" outcome=")[1].split(" ")[0] for line in _audit_lines(audit)] == [
            "authenticated",
            "TokenExpiredError",
        ]

    def test_a_refusal_before_logon_is_recorded(self, driver, audit):
        with pytest.raises(TokenExpiredError):
            _logon(_config(token=_jwt(-60)))
        assert "auth.method=jwt outcome=TokenExpiredError" in _audit_lines(audit)[0]


# --- AC 6.4: the token is never exposed ----------------------------------------


def test_no_secret_reaches_a_log_or_an_error(driver, audit, monkeypatch, caplog):
    token = _jwt(sub="LEAKED-SUBJECT")
    expired = _jwt(-60, sub="LEAKED-SUBJECT")
    secrets = [
        token,
        token.split(".")[1],
        expired.split(".")[1],
        CLIENT_SECRET,
        PASSWORD,
        "LEAKED-SUBJECT",
    ]
    rendered = []
    caplog.set_level(logging.DEBUG)

    def attempt(config, side_effect=None):
        driver.connect.side_effect = side_effect
        for connector in (_indexer(config), _uploader(config, table_name=None)):
            try:
                connector.precheck()
            except Exception as e:
                rendered.append("".join(traceback.format_exception(e)))

    attempt(_password_config(), _FakeTeradataDriverError(f"{_UNREACHABLE_TEXT} {PASSWORD}"))
    attempt(_config(token=token))
    attempt(_config(token=expired))
    attempt(_config(token=token), _FakeTeradataDriverError(f"{_REJECTED_TEXT} {token}"))
    attempt(_config(token=token), _FakeTeradataDriverError(f"{_EXPIRED_TEXT} {token}"))
    monkeypatch.setattr(
        requests,
        "post",
        lambda *a, **k: _token_response(
            {"error": "invalid_client", "error_description": CLIENT_SECRET}, status=401
        ),
    )
    attempt(_client_credentials_config())

    everything = caplog.text + "".join(rendered)
    assert rendered, "the failing attempts must have raised"
    for secret in secrets:
        assert secret not in everything


@pytest.mark.parametrize(
    "credentials",
    [
        pytest.param({"token": _jwt()}, id="jwt"),
        pytest.param(
            {"token_url": "https://idp.example/token", "client_id": "c", "client_secret": "s"},
            id="client_credentials",
        ),
    ],
)
def test_a_jwt_config_survives_pickling(credentials):
    """The default pipeline runs steps in worker processes, which pickles the connection
    config; a held threading.Lock made that raise TypeError before any document ran."""
    import pickle

    config = _config(**credentials)
    copy = pickle.loads(pickle.dumps(config))

    assert copy.auth_method == config.auth_method
    if "token" in credentials:
        assert copy._token.value == credentials["token"]
