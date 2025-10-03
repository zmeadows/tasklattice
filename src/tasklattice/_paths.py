# tasklattice/_paths.py
# Strong path types (internal) for TaskLattice.
#
# Goals:
# - Accept easy user inputs (str | os.PathLike[str]) at the edges.
# - Fail fast: validate & normalize once in constructors/classmethods.
# - Store stable forms:
#     * Top-level dirs/files as pathlib.Path
#     * In-layout paths as POSIX *relative* strings (no "..")
# - Don’t resolve symlinks by default.

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Self, TypeAlias

# ---------------------------------------------------------------------------
# Intent-only aliases for inputs. These don’t enforce absolute/relative
# at type-check time; constructors below do the runtime validation.
# ---------------------------------------------------------------------------
# TODO(@zmeadows): consolidate these to one UserPath
UserPath: TypeAlias = str | os.PathLike[str]


# ---------------------------------------------------------------------------
# RelPath: POSIX-style *relative* path used inside prototype/run layouts.
# Invariants:
#   - Not absolute (no leading "/" or "\" or UNC)
#   - Not drive-anchored (no "C:" prefix)
#   - No parent traversal ("..")
#   - No empty segments or "."
#   - Stored with forward slashes
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True, init=False)
class RelPath:
    """POSIX-style relative path (no leading '/', no '..'). Stored as a str."""

    value: str

    _DRIVE_ANCHOR = re.compile(r"^[A-Za-z]:")  # 'C:' etc.
    _UNC_PREFIXES = ("//", "\\\\")  # UNC prefixes

    def __init__(self, p: str | os.PathLike[str]):
        s_raw = os.fspath(p)
        if not isinstance(s_raw, str):
            raise TypeError(f"RelPath expects str-like, got {type(s_raw).__name__}")

        # Normalize separators
        s = s_raw.replace("\\", "/")

        # Reject absolute / anchored variants
        if s.startswith("/") or s.startswith(self._UNC_PREFIXES) or self._DRIVE_ANCHOR.match(s):
            raise ValueError(f"RelPath must be relative (got anchored/absolute): {s_raw!r}")

        # Collapse empty and '.' segments; forbid '..'
        parts = [seg for seg in s.split("/") if seg not in ("", ".")]
        if not parts:
            raise ValueError("RelPath may not be empty.")
        if any(seg == ".." for seg in parts):
            raise ValueError(f"RelPath may not contain '..': {s_raw!r}")

        object.__setattr__(self, "value", "/".join(parts))

    def __str__(self) -> str:
        return self.value

    def parts(self) -> tuple[str, ...]:
        return tuple(self.value.split("/"))

    def join_under(self, base: Path | AbsDir) -> Path:
        base_path = base.path if isinstance(base, AbsDir) else base
        return base_path.joinpath(*self.parts())


# ---------------------------------------------------------------------------
# AbsDir: absolute, normalized directory path.
#   - AbsDir.existing(...)   -> must exist & be a directory
#   - AbsDir.normalized(...) -> normalized path (may not exist yet)
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class AbsDir:
    path: Path

    @classmethod
    def _from(cls, p: UserPath, *, must_exist: bool) -> Self:
        # Normalize early
        q = Path(os.fspath(p)).expanduser()

        if must_exist:
            # Resolve fully; will raise if missing
            q = q.resolve(strict=True)
            if not q.is_dir():
                raise NotADirectoryError(str(q))
        else:
            # Make absolute & collapse ".." without requiring the leaf to exist
            q = (q if q.is_absolute() else (Path.cwd() / q)).resolve(strict=False)
            # If something is there already, it must be a directory
            if q.exists() and not q.is_dir():
                raise NotADirectoryError(str(q))

        return cls(q)

    @classmethod
    def existing(cls, p: UserPath) -> Self:
        return cls._from(p, must_exist=True)

    @classmethod
    def normalized(cls, p: UserPath) -> Self:
        return cls._from(p, must_exist=False)

    # Interop: allow passing to APIs that accept PathLike
    def __fspath__(self) -> str:
        return str(self.path)

    def __str__(self) -> str:
        return str(self.path)


# ---------------------------------------------------------------------------
# AbsFile: normalized *existing* file path (Path.is_file() follows symlinks).
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class AbsFile:
    path: Path

    @classmethod
    def existing(cls, p: UserPath, *, expand_user: bool = True) -> AbsFile:
        q = Path(os.fspath(p))
        if expand_user:
            q = q.expanduser()
        if not q.is_file():
            raise FileNotFoundError(q)
        return cls(q)

    def __fspath__(self) -> str:
        return str(self.path)

    def __str__(self) -> str:
        return str(self.path)
