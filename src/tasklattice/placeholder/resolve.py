from __future__ import annotations

from tasklattice.core import (
    Domain,
    DomainInterval,
    DomainIntervalUnresolved,
    DomainSet,
    DomainSetUnresolved,
    Number,
    SetLiteral,
    ValueLiteral,
    type_str_to_type_python,
)
from tasklattice.placeholder.model import ParamResolved, ParamUnresolved

_NUMERIC_TYPES: tuple[type, ...] = (int, float)


def _coerce_numeric(value: ValueLiteral, target: type[ValueLiteral]) -> Number:
    """
    Coerce numbers across int<->float when safe; never coerce to/from str or bool.

    Rules:
    - If target is float and value is int: cast to float.
    - If target is int and value is float: allow only integral floats (e.g., 3.0).
    - If target is bool or str: only accept exact instance match.
    """
    if target is float:
        if isinstance(value, bool):
            raise TypeError("Cannot coerce bool to float")
        if isinstance(value, int):
            return float(value)
        if isinstance(value, float):
            return value
        raise TypeError(f"Cannot coerce {type(value).__name__} to float")

    if target is int:
        if isinstance(value, bool):
            raise TypeError("Cannot coerce bool to int")
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            raise TypeError(f"Float {value} is not an integer; cannot coerce to int")
        raise TypeError(f"Cannot coerce {type(value).__name__} to int")

    raise TypeError(f"_coerce_numeric received non-numeric target {target!r}")


def _resolve_interval(
    dom: DomainIntervalUnresolved,
    target_type: type[ValueLiteral] | None,
) -> DomainInterval:
    inclusive_lower = dom.lpar == "["
    inclusive_upper = dom.rpar == "]"

    lower: Number = dom.lower
    upper: Number = dom.upper

    if target_type is not None:
        if target_type not in (int, float):
            raise TypeError("Interval domains are only valid for numeric types (int/float)")
        lower = _coerce_numeric(lower, target_type)
        upper = _coerce_numeric(upper, target_type)
        assert isinstance(lower, _NUMERIC_TYPES) and isinstance(upper, _NUMERIC_TYPES)

    # Validate ordering; equal bounds require both ends inclusive
    if (upper < lower) or (upper == lower and (not inclusive_lower or not inclusive_upper)):
        raise ValueError(f"Invalid interval domain: {dom.lpar}{dom.lower}, {dom.upper}{dom.rpar}")

    return DomainInterval(
        lower=lower,
        upper=upper,
        inclusive_lower=inclusive_lower,
        inclusive_upper=inclusive_upper,
    )


def _coerce_set_value(v: SetLiteral, target: type[ValueLiteral] | None) -> SetLiteral:
    if target is None:
        return v  # keep literal

    if target is str:
        if isinstance(v, str):
            return v
        raise TypeError(f"Set literal {v!r} must be a string for type 'str'")

    if target is bool:
        # Current grammar doesn't allow booleans in sets
        raise TypeError("DomainSet does not support 'bool' elements")

    if target is float:
        if isinstance(v, bool):
            raise TypeError("Cannot coerce bool to float in a set")
        if isinstance(v, int):
            return float(v)
        if isinstance(v, float):
            return v
        raise TypeError(f"Set literal {v!r} is not numeric for type 'float'")

    if target is int:
        if isinstance(v, bool):
            raise TypeError("Cannot coerce bool to int in a set")
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            if v.is_integer():
                return int(v)
            raise TypeError(f"Float {v} in set is not an integer; cannot coerce to int")
        raise TypeError(f"Set literal {v!r} is not numeric for type 'int'")

    raise TypeError(f"Unsupported target type {target!r} for DomainSet")


def _resolve_set(
    dom: DomainSetUnresolved,
    target_type: type[ValueLiteral] | None,
) -> DomainSet:
    values: set[SetLiteral] = set()
    for entry in dom.entries:
        values.add(_coerce_set_value(entry, target_type))
    return DomainSet(values=values)


def _infer_type_from_domain(
    dom: DomainIntervalUnresolved | DomainSetUnresolved,
) -> type[ValueLiteral]:
    if isinstance(dom, DomainIntervalUnresolved):
        # safest numeric superset; caller may downgrade to int using defaults/bounds
        return float

    all_str = all(isinstance(v, str) for v in dom.entries)
    all_num = all(isinstance(v, int | float) and not isinstance(v, bool) for v in dom.entries)

    if all_str:
        return str
    if all_num:
        return float if any(isinstance(v, float) for v in dom.entries) else int

    raise TypeError("Mixed-type DomainSet not supported for inference (strings and numbers)")


def _choose_type(
    user_type_raw: str | None,
    default: ValueLiteral,
    domain: DomainIntervalUnresolved | DomainSetUnresolved | None,
) -> type[ValueLiteral]:
    if user_type_raw is not None:
        t = type_str_to_type_python(user_type_raw)
        if t is None:
            raise RuntimeError(f"Unknown user-specified type label: {user_type_raw!r}")
        return t

    if domain is not None:
        inferred = _infer_type_from_domain(domain)
        # possible numeric downgrade to int if everything is integral
        if inferred is float and isinstance(default, int):
            if isinstance(domain, DomainIntervalUnresolved):
                if isinstance(domain.lower, int) and isinstance(domain.upper, int):
                    return int
            if isinstance(domain, DomainSetUnresolved):
                if all(isinstance(v, int) for v in domain.entries):
                    return int
        return inferred

    # Fall back to default literal type (preserve bool)
    if isinstance(default, bool):
        return bool
    elif isinstance(default, int):
        return int
    elif isinstance(default, float):
        return float
    elif isinstance(default, str):
        return str

    # raise TypeError(f"Unsupported default type: {type(default).__name__}")


def _coerce_default(default: ValueLiteral, target: type[ValueLiteral]) -> ValueLiteral:
    if target is bool:
        if isinstance(default, bool):
            return default
        raise TypeError("Default must be a bool for type 'bool'")
    if target is str:
        if isinstance(default, str):
            return default
        raise TypeError("Default must be a string for type 'str'")
    return _coerce_numeric(default, target)


def _resolve_domain(
    domain_unres: DomainIntervalUnresolved | DomainSetUnresolved | None,
    target_type: type[ValueLiteral] | None,
) -> Domain | None:
    if domain_unres is None:
        return None
    if isinstance(domain_unres, DomainIntervalUnresolved):
        return _resolve_interval(domain_unres, target_type)
    else:
        return _resolve_set(domain_unres, target_type)


def resolve_param(pu: ParamUnresolved) -> ParamResolved:
    """
    Core resolution function:
    - Choose final type (user-specified, or inferred from domain/default)
    - Coerce and validate default
    - Resolve and validate domain
    - Validate default ∈ domain if domain present
    """
    target_type = _choose_type(pu.py_type, pu.default, pu.domain)
    default_resolved = _coerce_default(pu.default, target_type)
    domain_resolved = _resolve_domain(pu.domain, target_type)

    if domain_resolved is not None and not domain_resolved.contains(default_resolved):
        raise ValueError(f"Default value {default_resolved!r} not within the specified domain")

    return ParamResolved(
        name=pu.name,
        default=default_resolved,
        domain=domain_resolved,
        description=pu.description,
        placeholder=pu.placeholder,
    )
