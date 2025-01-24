import json
from typing import IO, Any


def dumps(obj: list[dict[str, Any]], **kwargs) -> str:
    return "\n".join(json.dumps(each, **kwargs) for each in obj)


def dump(obj: list[dict[str, Any]], fp: IO, **kwargs) -> None:
    # Indent breaks ndjson formatting
    kwargs["indent"] = None
    text = dumps(obj, **kwargs)
    fp.write(text)


def loads(s: str, **kwargs) -> list[dict[str, Any]]:
    return [json.loads(line, **kwargs) for line in s.splitlines()]


def load(fp: IO, **kwargs) -> list[dict[str, Any]]:
    return loads(fp.read(), **kwargs)
