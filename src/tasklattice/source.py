from __future__ import annotations

from dataclasses import dataclass, field
from os import PathLike
from pathlib import Path
from typing import SupportsIndex


@dataclass(frozen=True, order=True, slots=True)
class SourceIndex:
    pos: int

    def __int__(self) -> int:
        return self.pos

    def __add__(self, n: SupportsIndex) -> SourceIndex:
        return SourceIndex(self.pos + int(n))

    def __sub__(self, other: SupportsIndex | SourceIndex) -> int | SourceIndex:
        if isinstance(other, SourceIndex):
            # distance
            return self.pos - other.pos
        return SourceIndex(self.pos - int(other))

    def __repr__(self) -> str:
        return f"SourceIndex({self.pos})"

@dataclass(frozen=True, slots=True)
class SourceSpan:
    '''0-indexed, [start, end) half-open interval for internal slicing.'''
    start: SourceIndex  # inclusive
    end: SourceIndex    # exclusive

    def __post_init__(self) -> None:
        if self.start < SourceIndex(0):
            raise ValueError(f"SourceSpan.start cannot be negative (got {self.start})")
        if self.end <= self.start:
            raise ValueError(f"SourceSpan.end ({self.end}) <= start ({self.start})")

    @classmethod
    def from_ints(cls, start: int, end: int) -> SourceSpan:
        return cls(SourceIndex(start), SourceIndex(end))

def _compute_line_starts(s: str) -> tuple[SourceIndex, ...]:
    # Start of each line (1st line starts at 0). Handles \n, \r\n, \r via splitlines.
    starts = [SourceIndex(0)]
    pos = 0
    for part in s.splitlines(keepends=True):
        pos += len(part)
        starts.append(SourceIndex(pos))
    return tuple(starts)

@dataclass(frozen=True, slots=True)
class Source:
    file: Path | None
    contents: str

    _line_starts: tuple[SourceIndex, ...] | None = field(
        default=None, init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if len(self.contents) == 0:
            if self.file is not None:
                raise ValueError(f"Empty file encountered: {self.file}.")
            else:
                raise ValueError("Empty source contents given.")

    @classmethod
    def from_file(cls, path_rep: str | Path | PathLike[str], encoding: str = "utf-8") -> Source:
        path = Path(path_rep)

        if not path.exists():
            raise FileNotFoundError(f"No such file: {path}")
        elif path.is_dir():
            raise IsADirectoryError(f"Expected a file but found a directory: {path}")

        try:
            return cls(path, path.read_text(encoding=encoding))
        except UnicodeDecodeError:
            raise
        except OSError as e:
            raise OSError(f"Failed to read file {path}: {e}") from e

    def full_span(self) -> SourceSpan:
        return SourceSpan.from_ints(0, len(self.contents))

    def slice(self, span: SourceSpan) -> str:
        if not (0 <= int(span.start) <= int(span.end) <= len(self.contents)):
            raise ValueError("SourceSpan out of bounds for this Source")
        return self.contents[int(span.start):int(span.end)]

    @property
    def line_starts(self) -> tuple[SourceIndex, ...]:
        ls = self._line_starts
        if ls is None:
            ls = _compute_line_starts(self.contents)
            # works for both frozen and non-frozen dataclasses
            object.__setattr__(self, "_line_starts", ls)
        return ls

    def pos_to_line_col(self, pos: SourceIndex) -> tuple[int, int]:
        '''returns 1-indexed (line, col), editor-style; accepts pos==len(contents).'''
        if not (0 <= int(pos) <= len(self.contents)):
            raise ValueError(f"pos {pos} out of range [0, {len(self.contents)}]")
        import bisect
        ls = self.line_starts
        line_idx = bisect.bisect_right(ls, pos) - 1
        return (line_idx + 1, int(pos - ls[line_idx]) + 1)  # 1-indexed

