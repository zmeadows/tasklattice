from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TypeAlias

from tasklattice.placeholder.model import PLACEHOLDER_RE, ParamName, ParamResolved, Placeholder
from tasklattice.placeholder.resolve import resolve_param
from tasklattice.placeholder.parse import parse_param
from tasklattice.source import Source, SourceIndex, SourceSpan

Parameters: TypeAlias = Mapping[ParamName, ParamResolved]
SequenceElement: TypeAlias = SourceSpan | ParamName
TemplateSequence: TypeAlias = tuple[SequenceElement, ...]

@dataclass(frozen=True, slots=True)
class Template:
    source: Source
    params: Parameters
    sequence: TemplateSequence

    @staticmethod
    def from_source(source: Source) -> Template:
        elements: list[SequenceElement] = []
        params: dict[ParamName, ParamResolved] = {}
        last = SourceIndex(0)

        def _append_span(start: SourceIndex, end: SourceIndex) -> None:
            nonlocal elements
            if end > start:
                elements.append(SourceSpan(start, end))

        for match in PLACEHOLDER_RE.finditer(source.contents):
            ph = Placeholder.from_match(source, match)
            _append_span(last, ph.span_outer.start)
            last = ph.span_outer.end

            par = resolve_param(parse_param(ph))

            if par.name in params:
                # TODO: better error message that points to both locations in Source
                raise RuntimeError(f"Duplicate parameter found with name: {par.name}")

            params[par.name] = par
            elements.append(par.name)

        _append_span(last, SourceIndex(len(source.contents)))

        return Template(
            source=source,
            params=MappingProxyType(params),
            sequence=tuple(elements),
        )
