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


class writer(object):
    def __init__(self, f, **kwargs):
        self.f = f
        self.kwargs = kwargs

    def write(self, row):
        stringified = json.dumps(row, **self.kwargs)
        self.f.write(stringified + "\n")


class reader(object):
    def __init__(self, f, **kwargs):
        self.f = f
        self.kwargs = kwargs

    def __iter__(self):
        return self

    def __next__(self):
        line = ""

        while line == "":
            line = next(self.f).strip()

        return json.loads(line, **self.kwargs)

    # NOTE: this is necessary to comply with py27
    def next(self):
        return self.__next__()
