import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import TypeAlias

from tasklattice.utils.fs_utils import ensure_parent_dirs

JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]
JSONObject: TypeAlias = dict[str, JSONValue]
JSONArray: TypeAlias = list[JSONValue]


def json_load(path: Path | str) -> JSONObject | None:
    try:
        with open(path, encoding="utf-8") as f:
            data: object = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, UnicodeDecodeError, OSError):
        return None
    return data if isinstance(data, dict) else None


def json_atomic_write(path: Path, payload: Mapping[str, object]) -> None:
    ensure_parent_dirs(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
