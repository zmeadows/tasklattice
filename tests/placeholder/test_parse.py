from __future__ import annotations

from typing import Any

from tasklattice.placeholder.source import Placeholder
from tasklattice.placeholder.parse import parse_param
from tasklattice.placeholder.model import Number

import pytest

@pytest.mark.parametrize("x", [1.5, -1.5, 0., int(0), int(-1), int(1)])
def test_number_smoke(x: Number) -> None:
    pu = parse_param(Placeholder.from_string(f"TL x = {x}"))
    assert pu.name == "x"
    #assert type(pu.default) is type(x)
    assert type(x) is type(pu.default)
    #from math import isclose
    #assert isclose(pu.default, x)
    assert pu.type_raw is None
    assert pu.domain_raw is None
    assert pu.desc is None

@pytest.mark.parametrize("x", ["", "asdf", "foo", "bar"])
def test_string_smoke(x: str) -> None:
    pu = parse_param(Placeholder.from_string(f"TL x = '{x}'"))
    assert pu.name == "x"
    assert type(pu.default) is str
    assert pu.default == x
    assert pu.type_raw is None
    assert pu.domain_raw is None
    assert pu.desc is None

@pytest.mark.parametrize(
    "domain_str,domain_parsed",
    [
        ("[0,3]", ["[", 0, 3, "]"]),
        ("(-3,3)", ["(", -3., 3., ")"]),
        ("(0,3]", ["(", 0., 3., "]"]),
        ("(-3,3]", ["(", -3., 3., "]"]),
    ]
)
def test_domain_smoke(domain_str: str, domain_parsed: list[Any]) -> None:
    pu = parse_param(Placeholder.from_string(f"TL x = 0., domain: {domain_str}"))
    assert pu.name == "x"
    assert type(pu.default) is float
    assert pu.default == 0.
    assert pu.type_raw is None
    assert pu.domain_raw == domain_parsed
    assert pu.desc is None
