from __future__ import annotations

import re
from dataclasses import dataclass, field

from tasklattice.core import (
    Domain,
    DomainIntervalUnresolved,
    DomainSetUnresolved,
    ParamName,
    ValueLiteral,
)

from tasklattice.placeholder.quotes import QuoteContext, detect_quote_context
from tasklattice.source import Source, SourceSpan
from tasklattice.profile import Profile

# Match: {{TL ...}}
# - allows whitespace after {{ 
# - body INCLUDES "TL" and runs up to the first "}}"
PLACEHOLDER_RE = re.compile(
    r"\{\{\s*(?P<body>TL\b(?:(?!\}\}).)*?)\}\}",
    re.DOTALL,
)

@dataclass(frozen=True, slots=True)
class Placeholder:
    source: Source
    span_outer: SourceSpan  # includes {{…}} but not surrounding quotes/whitespace
    span_inner: SourceSpan  # includes main parameter body text (TL ...) that we actually parse
    quote: QuoteContext | None # None if no symmetric quotes surround the placeholder

    @staticmethod
    def _construct(source: Source, span_outer: SourceSpan, span_inner: SourceSpan) -> Placeholder:
        return Placeholder(
            source=source,
            span_outer=span_outer,
            span_inner=span_inner,
            quote=detect_quote_context(source, span_outer),
        )

    @staticmethod
    def from_string(text: str, profile: Profile | None = None) -> Placeholder:
        m = PLACEHOLDER_RE.fullmatch(text)
        if not m:
            raise ValueError(f"Not a valid Placeholder string: {text!r}")

        return Placeholder._construct(
            source=Source.from_string(text, profile),
            span_outer=SourceSpan.from_ints(0, len(text)),
            span_inner=SourceSpan.from_ints(*m.span("body")),
        )

    @staticmethod
    def from_match(source: Source, m: re.Match[str]) -> Placeholder:
        return Placeholder._construct(
            source=source,
            span_outer=SourceSpan.from_ints(m.start(), m.end()),
            span_inner=SourceSpan.from_ints(*m.span("body")),
        )

    @property
    def text(self) -> str:
        return self.source.slice(self.span_inner)

    def line_col(self) -> tuple[int, int, int, int]:
        sl, sc = self.source.pos_to_line_col(self.span_outer.start)
        el, ec = self.source.pos_to_line_col(self.span_outer.end)
        return (sl, sc, el, ec)

    @property
    def fills_quotes(self) -> bool:
        # " {{TL x}} " is treated as “fills the quotes.”
        if not self.quote:
            return False
        interior = self.source.slice(self.quote.interior)
        return interior.strip() == self.source.slice(self.span_outer)

@dataclass(frozen=True, slots=True)
class ParamUnresolved:
    name: ParamName
    default: ValueLiteral
    placeholder: Placeholder
    py_type: str | None = None
    domain: DomainIntervalUnresolved | DomainSetUnresolved | None = None
    description: str | None = None

@dataclass(frozen=True, slots=True)
class ParamResolved:
    name: ParamName
    default: ValueLiteral
    placeholder: Placeholder
    py_type: type[ValueLiteral] = field(init=False, repr=True, compare=False)
    domain: Domain | None = None
    description: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "py_type", type(self.default))
