from __future__ import annotations

from tasklattice.placeholder.source import Placeholder
from tasklattice.placeholder.parse import parse_param
from tasklattice.placeholder.model import Number

import pytest

@pytest.mark.parametrize("x", [1.5, -1.5, 0., int(0), int(-1), int(1)])
def test_number_smoke(x: Number) -> None:
    from math import isclose

    pu = parse_param(Placeholder.from_string(f"TL x = {x}"))
    assert pu.name == "x"
    assert type(pu.value) is float
    assert isclose(pu.value, x)
    assert pu.type_raw is None
    assert pu.domain_raw is None
    assert pu.desc is None

@pytest.mark.parametrize("x", ["", "asdf", "foo", "bar"])
def test_string_smoke(x: str) -> None:
    pu = parse_param(Placeholder.from_string(f"TL x = '{x}'"))
    assert pu.name == "x"
    assert type(pu.value) is str
    assert pu.value == x
    assert pu.type_raw is None
    assert pu.domain_raw is None
    assert pu.desc is None
