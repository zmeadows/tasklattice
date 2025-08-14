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
class SourceOrigin:
    file: Path | None
    text: str


@dataclass(frozen=True, slots=True)
class SourceContext:
    origin: SourceOrigin
    span: SourceSpan

    # ---- Convenience: compute line/col only when rendering messages ----
    def line_col(self) -> tuple[int,int,int,int]:
        """
        Returns (start_line, start_col, end_line, end_col), 1-based like editors.
        """
        text = self.origin.text

        # TODO: naive but fine for moderate files; optimize if needed
        def to_line_col(pos: int) -> tuple[int, int]:
            # count '\n' before pos
            line_start = text.rfind("\n", 0, pos) + 1
            line_num = text.count("\n", 0, pos) + 1
            col_num = (pos - line_start) + 1
            return (line_num, col_num)

        sl, sc = to_line_col(self.span.start)
        el, ec = to_line_col(self.span.end)

        return (sl, sc, el, ec)

    def slice(self) -> str:
        return self.origin.text[self.span.start:self.span.end]

