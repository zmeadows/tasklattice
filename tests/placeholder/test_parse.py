from __future__ import annotations

from typing import Any

from tasklattice.placeholder.source import Placeholder
from tasklattice.placeholder.parse import parse_param
from tasklattice.placeholder.model import Number, Identifier, type_raw_to_python_type

import pytest

@pytest.mark.parametrize("x", [1.5, -1.5, 0., int(0), int(-1), int(1)])
def test_parse_number_smoke(x: Number) -> None:
    pu = parse_param(Placeholder.from_string(f"TL x = {x}"))
    assert pu.name == "x"
    assert type(x) is type(pu.default)
    assert pu.type_raw is None
    assert pu.domain_raw is None
    assert pu.description is None

@pytest.mark.parametrize("x", ["", "asdf", "foo", "bar"])
def test_parse_string_smoke(x: str) -> None:
    pu = parse_param(Placeholder.from_string(f"TL x = '{x}'"))
    assert pu.name == "x"
    assert type(pu.default) is str
    assert pu.default == x
    assert pu.type_raw is None
    assert pu.domain_raw is None
    assert pu.description is None

@pytest.mark.parametrize(
    "domain_str,domain_parsed,domain_type",
    [
        ("[0,3]", ["[", 0, 3, "]"], int),
        ("(-3.,3.)", ["(", -3., 3., ")"], float),
        ("(0,3]", ["(", 0, 3, "]"], int),
        ("(-3.,3.]", ["(", -3., 3., "]"], float),
    ]
)
def test_parse_domain_types_smoke(domain_str: str, domain_parsed: list[Any], domain_type: type) -> None:
    pu = parse_param(Placeholder.from_string(f"TL x = 0., domain: {domain_str}"))
    assert pu.name == "x"
    assert type(pu.domain_raw) is list
    assert type(pu.domain_raw[1]) is domain_type
    assert type(pu.domain_raw[2]) is domain_type
    assert type(pu.default) is float
    assert pu.default == 0.
    assert pu.type_raw is None
    assert pu.domain_raw == domain_parsed
    assert pu.description is None

def test_parse_bool_smoke() -> None:
    pu = parse_param(Placeholder.from_string(f"TL baz = true, desc: 'just some bool', type: bool"))

    assert pu.type_raw is not None
    assert pu.type_raw.value == "bool"
    type_actual = type_raw_to_python_type(pu.type_raw)
    assert type_actual is bool

    assert pu.name == "baz"
    assert type(pu.default) is type_actual
    assert pu.default == True
    assert type(pu.type_raw) is Identifier
    assert pu.domain_raw is None
    assert pu.description == "just some bool"

