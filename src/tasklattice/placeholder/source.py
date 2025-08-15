from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class SourceSpan:
    start: int  # inclusive
    end: int    # exclusive

    def __post_init__(self) -> None:
        if self.start < 0:
            raise ValueError(f"SourceSpan.start cannot be negative (got {self.start})")
        if self.end <= self.start:
            raise ValueError(f"SourceSpan.end ({self.end}) <= start ({self.start})")

@dataclass(frozen=True, slots=True)
class Source:
    file: Path | None
    contents: str

    def __post_init__(self) -> None:
        if len(self.contents) == 0:
            if self.file is not None:
                raise ValueError(f"Empty file encountered: {self.file}.")
            else:
                raise ValueError("Empty source contents given.")

    @staticmethod
    def create_from_file(path: Path, encoding: str = "utf-8") -> Source:
        if not path.exists():
            raise FileNotFoundError(f"No such file: {path}")
        if path.is_dir():
            raise IsADirectoryError(f"Expected a file but found a directory: {path}")

        try:
            return Source(path, path.read_text(encoding=encoding))
        except UnicodeDecodeError:
            raise
        except OSError as e:
            raise OSError(f"Failed to read file {path}: {e}") from e

    def full_span(self) -> SourceSpan:
        return SourceSpan(0, len(self.contents))

    def slice(self, span: SourceSpan) -> str:
        return self.contents[span.start:span.end]

@dataclass(frozen=True, slots=True)
class Placeholder:
    text: str
    source: Source
    span: SourceSpan

    @staticmethod
    def from_source(source: Source, span: SourceSpan) -> Placeholder:
        return Placeholder(source.slice(span), source, span)

    @staticmethod
    def from_string(text: str) -> Placeholder:
        source = Source(None, text)
        return Placeholder(text, source, source.full_span())

    def line_col(self) -> tuple[int,int,int,int]:
        """
        Returns (start_line, start_col, end_line, end_col), 1-indexed like editors.
        """
        text = self.source.contents

        # naive but fine for moderate files; optimize if needed
        def to_line_col(pos: int) -> tuple[int, int]:
            # count '\n' before pos
            line_start = text.rfind("\n", 0, pos) + 1
            line_num = text.count("\n", 0, pos) + 1
            col_num = (pos - line_start) + 1
            return (line_num, col_num)

        sl, sc = to_line_col(self.span.start)
        el, ec = to_line_col(self.span.end)

        return (sl, sc, el, ec)


