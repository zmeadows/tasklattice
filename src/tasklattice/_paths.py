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
from typing import TypeAlias

# ---------------------------------------------------------------------------
# Intent-only aliases for inputs. These don’t enforce absolute/relative
# at type-check time; constructors below do the runtime validation.
# ---------------------------------------------------------------------------
UserAbsPath: TypeAlias = str | os.PathLike[str]
UserRelPath: TypeAlias = str | os.PathLike[str]


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
# AbsDir: normalized directory path.
# Use factories to control validation:
#   - AbsDir.existing(...)  -> must exist & be a directory
#   - AbsDir.any(...)       -> normalized path (may not exist yet)
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class AbsDir:
    path: Path

    @staticmethod
    def existing(p: UserAbsPath, *, expand_user: bool = True) -> AbsDir:
        q = Path(os.fspath(p))
        if expand_user:
            q = q.expanduser()
        if not q.exists():
            raise FileNotFoundError(q)
        if not q.is_dir():
            raise NotADirectoryError(q)
        return AbsDir(q)

    @staticmethod
    def any(p: UserAbsPath, *, expand_user: bool = True) -> AbsDir:
        q = Path(os.fspath(p))
        return AbsDir(q.expanduser() if expand_user else q)

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
    def existing(cls, p: UserAbsPath, *, expand_user: bool = True) -> AbsFile:
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
