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

# [ FUTURE TESTS ]
# --- Happy-path parsing of simple params ---
# - parse_param_str("{{TL x = 1}}") → ParamUnresolved(name="x", default=1, py_type=None, domain=None, description=None)
# - Same with float: "{{TL x = 1.5}}"
# - Scientific notation: "{{TL x = 1e-3}}", signed numbers: "{{TL x = -2}}", "{{TL x = +4.0}}"
# - Booleans (case-insensitive): "{{TL flag = true}}", "{{TL flag = FALSE}}"
# - Strings unquoted via literal_eval: double-quoted and single-quoted, with escapes: r'{{TL s = "a\"b"}}', "{{TL s = 'a\\nb'}}"
#
# --- Whitespace & formatting robustness ---
# - Internal spacing variants all parse the same: "{{   TL   x   =   1   }}", "{{TL x=1}}", "{{TL    x=1}}"
# - Newlines inside placeholder: "{{TL x =\n 1\n}}"
#
# --- Meta pairs (type/domain/desc) ---
# - Single meta: "{{TL x = 1, type: float}}"
# - Multiple meta, any order: "{{TL x = 1, desc: 'ok', type: float, domain: (0, 1]}}"
# - Duplicate meta key → raises ValueError: "{{TL x = 1, type: float, type: int}}"
# - Unknown meta key → raises ValueError: "{{TL x = 1, typo: 123}}"
# - Description STRING unquotes correctly and preserves punctuation/whitespace
#
# --- Domain: intervals ---
# - Open/closed ends preserved in lpar/rpar:
#   - "{{TL x = 0, domain: (0, 1)}}" → lpar=="(", rpar==")", lower==0, upper==1
#   - "{{TL x = 0, domain: [0, 1]}}" → lpar=="[", rpar=="]"
# - Mixed brackets: "(0, 1]" and "[0, 1)"
# - Whitespace around comma tolerated: "(0 , 1)"
#
# --- Domain: sets ---
# - Non-empty set of numbers: "{{TL x = 0, domain: {1,2,3}}}"
# - Mixed literals if allowed by model (numbers + strings): "{{TL x = 0, domain: {1,'a',2}}}"
# - Empty set: "{{TL x = 0, domain: {}}}"
# - Strings with commas/braces inside quotes remain one element: "{{TL x = 0, domain: {'a,b}', 'c}d'}}"
#
# --- IDENTIFIER handling ---
# - Param name with underscores/digits (not starting with digit): "{{TL foo_bar9 = 1}}"
# - Ensure case sensitivity as intended (e.g., NAME vs name if relevant to your model)
#
# --- start/param transformer integration ---
# - Minimal placeholder returns ParamUnresolved even though start() uses items[2] indexing
#   (guards against regressions if grammar changes order).
# - If you change grammar to wrap with param rule: verify transformer still returns correct ParamUnresolved.
#
# --- Placeholder.from_string & parse_param_str ---
# - Valid placeholder passes through: parse_param_str("{{TL x = 1}}") equals parse_param(Placeholder.from_string(...))
# - Invalid placeholder strings raise (ValueError from from_string): "{{ x = 1 }}", "{{TL}}", "{{TL x}}", "TL x = 1"
# - PLACEHOLDER with lower-case "tl" should fail if keyword is case-sensitive: "{{tl x = 1}}"
#
# --- Error surfaces from parser ---
# - Malformed interval: "{{TL x = 0, domain: (0 1)}}" → Lark error (missing comma)
# - Malformed set: "{{TL x = 0, domain: {1,}}}" → Lark error (trailing comma not allowed by grammar)
#
# --- Unicode & escapes ---
# - Unicode identifier if allowed: "{{TL tên = 1}}" (or confirm it’s rejected if IDENTIFIER forbids)
# - Unicode string literal: "{{TL s = 'μ'}}"
#
# --- Stability & invariants of ParamUnresolved ---
# - py_type is None when missing; equals IDENTIFIER string when present (e.g., "float")
# - domain is DomainIntervalUnresolved or DomainSetUnresolved when provided; None when absent
# - description is None when absent; exact string when provided
#
# --- Round-trip sanity (optional) ---
# - Build many variants and assert transforming back to dict-like data (name/default/meta) stays consistent across spacing/ordering
