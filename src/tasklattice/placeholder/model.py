from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from tasklattice.source import Source, SourceSpan


@dataclass(frozen=True, slots=True)
class Placeholder:
    text: str
    source: Source
    span: SourceSpan

    @staticmethod
    def from_string(text: str) -> Placeholder:
        source = Source(None, text)
        return Placeholder(text, source, source.full_span())

    def line_col(self) -> tuple[int, int, int, int]:
        sl, sc = self.source.pos_to_line_col(self.span.start)
        el, ec = self.source.pos_to_line_col(self.span.end)
        return (sl, sc, el, ec)


Number = int | float

Literal = str | Number | bool

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
    def contains(self, value: Literal) -> bool:
        ...

@dataclass(frozen=True, slots=True)
class DomainInterval(Domain):
    lower: Number
    upper: Number
    inclusive_lower: bool
    inclusive_upper: bool

    def contains(self, value: Literal) -> bool:
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
    default: Literal
    py_type: str | None = None
    domain: DomainIntervalUnresolved | DomainSetUnresolved | None = None
    description: str | None = None

@dataclass(frozen=True, slots=True)
class ParamResolved:
    name: ParamName
    default: Literal
    py_type: type[Literal] = field(init=False, repr=True, compare=False)
    domain: Domain | None = None
    description: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "py_type", type(self.default))

