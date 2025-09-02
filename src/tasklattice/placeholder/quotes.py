from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from tasklattice.source import Source, SourceIndex, SourceSpan

QuoteType = Literal["single", "double"]

@dataclass(frozen=True, slots=True)
class QuoteContext:
    style: QuoteType
    left_index: SourceIndex
    right_index: SourceIndex

    @property
    def exterior(self) -> SourceSpan:
        return SourceSpan(self.left_index, self.right_index + 1)

    @property
    def interior(self) -> SourceSpan:
        return SourceSpan(self.left_index + 1, self.right_index)

def _skip_ws_left(text: str, i: SourceIndex) -> SourceIndex:
    j = i
    while int(j) >= 0 and text[int(j)].isspace():
        j = j - 1
    return j

def _skip_ws_right(text: str, i: SourceIndex) -> SourceIndex:
    j = i
    n = len(text)
    while int(j) < n and text[int(j)].isspace():
        j = j + 1
    return j

def _as_quote_type(ch: str) -> QuoteType | None:
    if ch == '"':
        return "double"
    elif ch == "'":
        return "single"
    else:
        return None

def detect_quote_context(source: Source, span_outer: SourceSpan) -> QuoteContext | None:
    """
    Return QuoteContext if `span_outer` is immediately surrounded by symmetric quotes,
    allowing only whitespace between the quotes and the braces; otherwise None.

    Heuristic only; we don't parse host-language escapes or nesting.
    """
    text = source.contents
    n = len(text)
    if n == 0 or span_outer.start >= span_outer.end:
        return None

    # cannot have quotes if placeholder touches buffer edges
    if int(span_outer.start) == 0 or int(span_outer.end) >= n:
        return None

    left_scan = _skip_ws_left(text, span_outer.start - 1)
    right_scan = _skip_ws_right(text, span_outer.end)
    if int(left_scan) < 0 or int(right_scan) >= n:
        return None

    left_ch = text[int(left_scan)]
    right_ch = text[int(right_scan)]
    left_style = _as_quote_type(left_ch)
    right_style = _as_quote_type(right_ch)
    if left_style is None or right_style is None or left_style != right_style:
        return None

    # very light escape guard; drop if you don't want to guess about escapes
    if int(left_scan) > 0 and text[int(left_scan) - 1] == "\\":
        return None
    if int(right_scan) > 0 and text[int(right_scan) - 1] == "\\":
        return None

    return QuoteContext(
        style=left_style,
        left_index=left_scan,
        right_index=right_scan
    )

