from abc import ABC, abstractmethod
from dataclasses import dataclass, field

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

@dataclass(frozen=True, slots=True)
class ParamUnresolved:
    name: str
    default: Literal
    py_type: str | None = None
    domain: DomainIntervalUnresolved | DomainSetUnresolved | None = None
    description: str | None = None

@dataclass(frozen=True, slots=True)
class ParamResolved:
    name: str
    default: Literal
    py_type: type = field(init=False, repr=True, compare=False)
    domain: Domain | None = None
    description: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "py_type", type(self.default))
