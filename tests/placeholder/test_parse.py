from __future__ import annotations

from tasklattice.placeholder.parse import parse_param_str
from tasklattice.placeholder.model import Number, type_raw_to_python_type, DomainIntervalUnresolved

from tests.utils import tl

import pytest

@pytest.mark.parametrize("x", [1.5, -1.5, 0., int(0), int(-1), int(1)])
def test_parse_number_smoke(x: Number) -> None:
    pu = parse_param_str(tl(f"TL x = {x}"))
    assert str(pu.name) == "x"
    assert type(x) is type(pu.default)
    assert pu.py_type is None
    assert pu.domain is None
    assert pu.description is None

@pytest.mark.parametrize("x", ["", "asdf", "foo", "bar"])
def test_parse_string_smoke(x: str) -> None:
    pu = parse_param_str(tl(f"TL x = '{x}'"))
    assert str(pu.name) == "x"
    assert type(pu.default) is str
    assert pu.default == x
    assert pu.py_type is None
    assert pu.domain is None
    assert pu.description is None

@pytest.mark.parametrize(
    "domain_str,domain_parsed,domain_type",
    [
        ("[0,3]", DomainIntervalUnresolved(0,3,"[","]"), int),
        ("[-3.,3.]", DomainIntervalUnresolved(-3.,3.,"[","]"), float),
        ("(0,3]", DomainIntervalUnresolved(0,3,"(","]"), int),
        ("(-3.,3.]", DomainIntervalUnresolved(-3.,3.,"(","]"), float),
    ]
)
def test_parse_domain_types_smoke(
        domain_str: str,
        domain_parsed: DomainIntervalUnresolved,
        domain_type: type) -> None:
    pu = parse_param_str(tl(f"TL x = 0., domain: {domain_str}"))
    assert str(pu.name) == "x"
    assert type(pu.domain) is DomainIntervalUnresolved
    assert type(pu.domain.lower) is domain_type
    assert type(pu.domain.upper) is domain_type
    assert type(pu.default) is float
    assert pu.default == 0.
    assert pu.py_type is None
    assert pu.domain == domain_parsed
    assert pu.description is None

def test_parse_bool_smoke() -> None:
    pu = parse_param_str(tl(f"TL baz = true, desc: 'just some bool', type: bool"))

    assert pu.py_type is not None
    assert pu.py_type == "bool"
    type_actual = type_raw_to_python_type(pu.py_type)
    assert type_actual is bool

    assert str(pu.name) == "baz"
    assert type(pu.default) is type_actual
    assert pu.default == True
    assert type(pu.py_type) is str
    assert pu.domain is None
    assert pu.description == "just some bool"

