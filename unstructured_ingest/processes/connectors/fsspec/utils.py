import json
from datetime import datetime
from pathlib import Path
from typing import Callable


def json_serial(obj):
    if isinstance(obj, Path):
        return obj.as_posix()
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def sterilize_dict(data: dict, default: Callable = json_serial) -> dict:
    data_s = json.dumps(data, default=default)
    return json.loads(data_s)


def get_size(file_path: str) -> int:
    path = Path(file_path)
    return path.stat().st_size


def add_suffix(filename: str, number: int) -> str:
    if "." in filename:
        name, ext = filename.rsplit(".", 1)
        return f"{name}({number}).{ext}"
    else:
        return f"{filename}({number})"
