from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Literal

from tasklattice.source import Source, SourceIndex, SourceSpan

# Match: {{TL ...}}
# - allows whitespace after {{ 
# - body INCLUDES "TL" and runs up to the first "}}"
PLACEHOLDER_RE = re.compile(
    r"\{\{\s*(?P<body>TL\b(?:(?!\}\}).)*?)\}\}",
    re.DOTALL,
)

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
            quote=None,
        )

    @staticmethod
    def from_string(text: str) -> Placeholder:
        # TODO: Allow optional profile (xml, json, etc) for testing
        m = PLACEHOLDER_RE.fullmatch(text)
        if not m:
            raise ValueError(f"Not a valid Placeholder string: {text!r}")

        return Placeholder._construct(
            source=Source(file=None, contents=text),
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
        if not self.quote:
            return False
        interior = self.source.slice(self.quote.interior)
        return interior.strip() == self.source.slice(self.span_outer)

Number = int | float

ValueLiteral = str | Number | bool

SetLiteral = str | Number

# TODO: rename
def type_raw_to_python_type(type_raw: str) -> type | None:
    TYPE_MAP = {
        "str" : str,
        "int" : int,
        "float" : float,
        "bool" : bool,
    }

    return TYPE_MAP.get(type_raw, None)

class Domain(ABC):
    @abstractmethod
    def contains(self, value: ValueLiteral) -> bool:
        ...

@dataclass(frozen=True, slots=True)
class DomainInterval(Domain):
    lower: Number
    upper: Number
    inclusive_lower: bool
    inclusive_upper: bool

    def contains(self, value: ValueLiteral) -> bool:
        if not isinstance(value, Number) or isinstance(value, bool):
            return False

        if (value < self.lower) or (value == self.lower and not self.inclusive_lower):
            return False
        if (value > self.upper) or (value == self.upper and not self.inclusive_upper):
            return False

        return True

@dataclass(frozen=True, slots=True)
class DomainIntervalUnresolved:
    lower: Number
    upper: Number
    lpar: str
    rpar: str

@dataclass(frozen=True, slots=True)
class DomainSet(Domain):
    values: set[SetLiteral]

    def contains(self, value: SetLiteral) -> bool:
        if isinstance(value, bool):
            # TODO: rethink behavior here and capture in test suite
            # Require exact identity match for bools
            return any(v is value for v in self.values)
        return value in self.values

@dataclass(frozen=True, slots=True)
class DomainSetUnresolved:
    entries: list[SetLiteral]


# IDENTIFIER: /[A-Za-z_][A-Za-z0-9_]*/
_PARAM_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\Z")

@dataclass(frozen=True, slots=True)
class ParamName:
    value: str

    def __post_init__(self) -> None:
        if not _PARAM_NAME_RE.match(self.value):
            raise ValueError(
                f"Invalid ParamName {self.value!r}: must match {_PARAM_NAME_RE.pattern!r}"
            )

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return f"ParamName({self.value!r})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ParamName) and self.value == other.value

    def __hash__(self) -> int:
        return hash(self.value)

@dataclass(frozen=True, slots=True)
class ParamUnresolved:
    name: ParamName
    default: ValueLiteral
    py_type: str | None = None
    domain: DomainIntervalUnresolved | DomainSetUnresolved | None = None
    description: str | None = None

@dataclass(frozen=True, slots=True)
class ParamResolved:
    name: ParamName
    default: ValueLiteral
    py_type: type[ValueLiteral] = field(init=False, repr=True, compare=False)
    domain: Domain | None = None
    description: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "py_type", type(self.default))

