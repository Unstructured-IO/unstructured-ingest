import base64
import json
import zlib
from datetime import datetime
from typing import Any

import pytest
import pytz
from pydantic import BaseModel, Field, Secret, SecretStr
from pydantic.types import _SecretBase

from unstructured_ingest.processes.connectors.utils import format_and_truncate_orig_elements
from unstructured_ingest.utils.pydantic_models import (
    serialize_base_model,
    serialize_base_model_json,
)
from unstructured_ingest.utils.string_and_date_utils import (
    ensure_isoformat_datetime,
    fix_unescaped_unicode,
    json_to_dict,
    truncate_string_bytes,
)

flat_data = {"a": "test", "b": 4, "c": True}


def test_json_to_dict_valid_json():
    json_string = '{"key": "value"}'
    expected_result = {"key": "value"}
    assert json_to_dict(json_string) == expected_result
    assert isinstance(json_to_dict(json_string), dict)


def test_json_to_dict_malformed_json():
    json_string = '{"key": "value"'
    expected_result = '{"key": "value"'
    assert json_to_dict(json_string) == expected_result
    assert isinstance(json_to_dict(json_string), str)


def test_json_to_dict_single_quotes():
    json_string = "{'key': 'value'}"
    expected_result = {"key": "value"}
    assert json_to_dict(json_string) == expected_result
    assert isinstance(json_to_dict(json_string), dict)


def test_json_to_dict_path():
    json_string = "/path/to/file.json"
    expected_result = "/path/to/file.json"
    assert json_to_dict(json_string) == expected_result
    assert isinstance(json_to_dict(json_string), str)


def test_ensure_isoformat_datetime_for_datetime():
    dt = ensure_isoformat_datetime(datetime(2021, 1, 1, 12, 0, 0))
    assert dt == "2021-01-01T12:00:00"


def test_ensure_isoformat_datetime_for_datetime_with_tz():
    dt = ensure_isoformat_datetime(datetime(2021, 1, 1, 12, 0, 0, tzinfo=pytz.UTC))
    assert dt == "2021-01-01T12:00:00+00:00"


def test_ensure_isoformat_datetime_for_string():
    dt = ensure_isoformat_datetime("2021-01-01T12:00:00")
    assert dt == "2021-01-01T12:00:00"


def test_ensure_isoformat_datetime_for_string2():
    dt = ensure_isoformat_datetime("2021-01-01T12:00:00+00:00")
    assert dt == "2021-01-01T12:00:00+00:00"


def test_ensure_isoformat_datetime_fails_on_string():
    with pytest.raises(ValueError):
        ensure_isoformat_datetime("bad timestamp")


def test_ensure_isoformat_datetime_fails_on_int():
    with pytest.raises(TypeError):
        ensure_isoformat_datetime(1111)


def test_truncate_string_bytes_return_truncated_string():
    test_string = "abcdef안녕하세요ghijklmn방갑습니opqrstu 더 길어지면 안되는 문자열vwxyz"
    max_bytes = 11
    result = truncate_string_bytes(test_string, max_bytes)
    assert result == "abcdef안"
    assert len(result.encode("utf-8")) <= max_bytes


def test_truncate_string_bytes_return_untouched_string():
    test_string = "abcdef"
    max_bytes = 11
    result = truncate_string_bytes(test_string, max_bytes)
    assert result == "abcdef"
    assert len(result.encode("utf-8")) <= max_bytes


def test_fix_unescaped_unicode_valid():
    text = "This is a test with unescaped unicode: \\u0041"
    expected = "This is a test with unescaped unicode: \u0041"
    assert fix_unescaped_unicode(text) == expected


def test_fix_unescaped_unicode_no_unescaped_chars():
    text = "This is a test with no unescaped unicode: \u0041"
    expected = "This is a test with no unescaped unicode: \u0041"
    assert fix_unescaped_unicode(text) == expected


def test_fix_unescaped_unicode_invalid_unicode():
    text = "This is a test with invalid unescaped unicode: \\uZZZZ"
    expected = "This is a test with invalid unescaped unicode: \\uZZZZ"
    assert fix_unescaped_unicode(text) == expected


def test_fix_unescaped_unicode_encoding_error(caplog: pytest.LogCaptureFixture):
    text = "This is a test with unescaped unicode: \\uD83D"
    fix_unescaped_unicode(text)
    with caplog.at_level("WARNING"):
        fix_unescaped_unicode(text)
        assert "Failed to fix unescaped Unicode sequences" in caplog.text


class MockChildBaseModel(BaseModel):
    child_secret_str: SecretStr
    child_secret_float: Secret[float]
    child_not_secret_dict: dict[str, Any] = Field(default_factory=dict)


class MockBaseModel(BaseModel):
    secret_str: SecretStr
    not_secret_bool: bool
    secret_child_base: Secret[MockChildBaseModel]
    not_secret_list: list[int] = Field(default_factory=list)


model = MockBaseModel(
    secret_str="secret string",
    not_secret_bool=False,
    secret_child_base=MockChildBaseModel(
        child_secret_str="child secret string",
        child_secret_float=3.14,
        child_not_secret_dict={"key": "value"},
    ),
    not_secret_list=[1, 2, 3],
)


def test_serialize_base_model():

    serialized_dict = model.model_dump()
    assert isinstance(serialized_dict["secret_str"], _SecretBase)
    assert isinstance(serialized_dict["secret_child_base"], _SecretBase)

    serialized_dict_w_secrets = serialize_base_model(model=model)
    assert not isinstance(serialized_dict_w_secrets["secret_str"], _SecretBase)
    assert not isinstance(serialized_dict_w_secrets["secret_child_base"], _SecretBase)

    expected_dict = {
        "secret_str": "secret string",
        "not_secret_bool": False,
        "secret_child_base": {
            "child_secret_str": "child secret string",
            "child_secret_float": 3.14,
            "child_not_secret_dict": {"key": "value"},
        },
        "not_secret_list": [1, 2, 3],
    }

    assert serialized_dict_w_secrets == expected_dict


def test_serialize_base_model_json():
    serialized_json = model.model_dump_json()
    serialized_dict = json.loads(serialized_json)
    expected_dict = {
        "secret_str": "**********",
        "not_secret_bool": False,
        "secret_child_base": "**********",
        "not_secret_list": [1, 2, 3],
    }
    assert expected_dict == serialized_dict

    serialized_json_w_secrets = serialize_base_model_json(model=model)
    serialized_dict_w_secrets = json.loads(serialized_json_w_secrets)
    expected_dict_w_secrets = {
        "secret_str": "secret string",
        "not_secret_bool": False,
        "secret_child_base": {
            "child_secret_str": "child secret string",
            "child_secret_float": 3.14,
            "child_not_secret_dict": {"key": "value"},
        },
        "not_secret_list": [1, 2, 3],
    }
    assert expected_dict_w_secrets == serialized_dict_w_secrets


def test_format_and_truncate_orig_elements():
    original_elements = [
        {
            "text": "Hello, world!",
            "metadata": {
                "image_base64": "iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAABwUlEQVR42mNk",
                "text_as_html": "<p>Hello, world!</p>",
                "page": 1,
            },
        }
    ]
    json_bytes = json.dumps(original_elements, sort_keys=True).encode("utf-8")
    deflated_bytes = zlib.compress(json_bytes)
    b64_deflated_bytes = base64.b64encode(deflated_bytes)
    b64_deflated_bytes.decode("utf-8")

    assert format_and_truncate_orig_elements(
        {"text": "Hello, world!", "metadata": {"orig_elements": b64_deflated_bytes.decode("utf-8")}}
    ) == [{"metadata": {"page": 1}}]
