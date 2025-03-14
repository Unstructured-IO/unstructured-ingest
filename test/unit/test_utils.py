import json
import typing as t
from dataclasses import dataclass, field
from datetime import datetime

import pytest
import pytz

from unstructured_ingest.cli.utils import extract_config
from unstructured_ingest.interfaces import BaseConfig
from unstructured_ingest.utils.string_and_date_utils import (
    ensure_isoformat_datetime,
    fix_unescaped_unicode,
    json_to_dict,
    truncate_string_bytes,
)


@dataclass
class A(BaseConfig):
    a: str


@dataclass
class B(BaseConfig):
    a: A
    b: int


flat_data = {"a": "test", "b": 4, "c": True}


def test_extract_config_concrete():
    @dataclass
    class C(BaseConfig):
        b: B
        c: bool

    c = extract_config(flat_data=flat_data, config=C)
    expected_result = {"b": {"a": {"a": "test"}, "b": 4}, "c": True}
    assert c.to_json(sort_keys=True) == json.dumps(expected_result, sort_keys=True)


def test_extract_config_optional():
    @dataclass
    class C(BaseConfig):
        c: bool
        b: t.Optional[B] = None

    c = extract_config(flat_data=flat_data, config=C)
    expected_result = {"b": {"a": {"a": "test"}, "b": 4}, "c": True}
    assert c.to_json(sort_keys=True) == json.dumps(expected_result, sort_keys=True)


def test_extract_config_union():
    @dataclass
    class C(BaseConfig):
        c: bool
        b: t.Optional[t.Union[B, int]] = None

    c = extract_config(flat_data=flat_data, config=C)
    expected_result = {"b": 4, "c": True}
    assert c.to_json(sort_keys=True) == json.dumps(expected_result, sort_keys=True)


def test_extract_config_list():
    @dataclass
    class C(BaseConfig):
        c: t.List[int]
        b: B

    flat_data = {"a": "test", "b": 4, "c": [1, 2, 3]}
    c = extract_config(flat_data=flat_data, config=C)
    expected_result = {"b": {"a": {"a": "test"}, "b": 4}, "c": [1, 2, 3]}
    assert c.to_json(sort_keys=True) == json.dumps(expected_result, sort_keys=True)


def test_extract_config_optional_list():
    @dataclass
    class C(BaseConfig):
        b: B
        c: t.Optional[t.List[int]] = None

    flat_data = {"a": "test", "b": 4, "c": [1, 2, 3]}
    c = extract_config(flat_data=flat_data, config=C)
    expected_result = {"b": {"a": {"a": "test"}, "b": 4}, "c": [1, 2, 3]}
    assert c.to_json(sort_keys=True) == json.dumps(expected_result, sort_keys=True)


def test_extract_config_dataclass_list():
    @dataclass
    class C(BaseConfig):
        c: bool
        b: t.List[B] = field(default_factory=list)

    flat_data = {"a": "test", "c": True}
    c = extract_config(flat_data=flat_data, config=C)
    expected_result = {"b": [], "c": True}
    assert c.to_json(sort_keys=True) == json.dumps(expected_result, sort_keys=True)


def test_extract_config_dict():
    @dataclass
    class C(BaseConfig):
        c: bool
        b: t.Dict[str, B] = field(default_factory=dict)

    flat_data = {"c": True}
    c = extract_config(flat_data=flat_data, config=C)
    expected_result = {"c": True, "b": {}}
    assert c.to_json(sort_keys=True) == json.dumps(expected_result, sort_keys=True)


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
