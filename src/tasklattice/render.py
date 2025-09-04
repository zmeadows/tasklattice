from __future__ import annotations

from tasklattice.source import SourceSpan
from tasklattice.template import Template
from tasklattice.placeholder.model import ValueLiteral, SubstitutionMap, ParamResolved


def _validate_map(tpt: Template, subs: SubstitutionMap) -> None:
    for sname, svalue in subs.items():
        param = tpt.params.get(sname, None)
        if param is None:
            raise RuntimeError(f"Parameter name not found: {sname}")
        if param.domain is not None and not param.domain.contains(svalue):
            raise RuntimeError(f"Domain for parameter {sname} does not contain value: {svalue}")
        if not isinstance(svalue, param.py_type):
            actual_type = type(svalue)
            raise RuntimeError(f"Attempted to substitution value of type {actual_type} for parameter of type: {param.py_type}")

def _render_literal(param: ParamResolved, val: ValueLiteral) -> str:
    # TODO:
    return ""

def render(tpt: Template, subs: SubstitutionMap) -> str:
    _validate_map(tpt, subs)

    chunks = []
    for selem in tpt.sequence:
        if isinstance(selem, SourceSpan):
            chunks.append(tpt.source.slice(selem))
        else:
            pr = tpt.params[selem]
            val = subs.get(selem, pr.default)
            chunks.append(_render_literal(pr, val))

    return ''.join(chunks)



