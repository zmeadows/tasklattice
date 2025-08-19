from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class Identifier:
    value: str

Literal = str | int | float | bool

Number = int | float

def type_raw_to_python_type(type_raw: Identifier) -> type | None:
    TYPE_MAP = {
        "str" : str,
        "int" : int,
        "float" : float,
        "bool" : bool,
    }

    return TYPE_MAP.get(type_raw.value, None)

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
        if not isinstance(value, int | float) or isinstance(value, bool):
            return False

        if (value < self.lower) or (value == self.lower and not self.inclusive_lower):
            return False
        if (value > self.upper) or (value == self.upper and not self.inclusive_upper):
            return False

        return True

@dataclass(frozen=True, slots=True)
class DomainSet(Domain):
    values: set[Literal]

    def contains(self, value: Literal) -> bool:
        if isinstance(value, bool):
            # Require exact identity match for bools
            return any(v is value for v in self.values)
        return value in self.values

@dataclass(frozen=True, slots=True)
class ParamUnresolved:
    name: str
    default: Literal
    type_raw: Identifier | None = None
    domain_raw: list[Any] | None = None
    description: str | None = None

@dataclass(frozen=True, slots=True)
class ParamResolved:
    name: str
    default: Literal
    domain: Domain | None = None
    description: str | None = None

    def type(self) -> type:
        return type(self.default)
