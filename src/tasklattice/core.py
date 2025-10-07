from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TypeAlias


class QuoteStyle(StrEnum):
    SINGLE = "single"
    DOUBLE = "double"


Number: TypeAlias = int | float

ValueLiteral: TypeAlias = str | Number | bool

SetLiteral: TypeAlias = str | Number


def type_str_to_type_python(type_str: str) -> type[ValueLiteral] | None:
    TYPE_MAP = {
        "str": str,
        "int": int,
        "float": float,
        "bool": bool,
    }

    return TYPE_MAP.get(type_str, None)


class Domain(ABC):
    @abstractmethod
    def contains(self, value: ValueLiteral) -> bool: ...


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


SubstitutionMap: TypeAlias = Mapping[ParamName, ValueLiteral]
