import json
import os
from pathlib import Path
from typing import Any, cast

from tasklattice.utils.fs_utils import ensure_parent_dirs


def json_load(path: Path) -> dict[str, Any] | None:
    try:
        with path.open("r", encoding="utf-8") as f:
            data: Any = json.load(f)
    except FileNotFoundError:
        return None
    except Exception:
        # Corrupt or unreadable; treat as missing.
        return None

    if isinstance(data, dict):
        # JSON object keys are strings per spec; cast is safe here.
        return cast(dict[str, Any], data)

    # If it wasn't a JSON object (e.g., list/str), ignore it.
    return None


def json_atomic_write(path: Path, payload: dict[str, Any]) -> None:
    ensure_parent_dirs(path)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)
