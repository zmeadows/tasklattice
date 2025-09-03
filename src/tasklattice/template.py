from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TypeAlias

from .placeholder.model import PLACEHOLDER_RE, ParamName, ParamResolved, Placeholder, ValueLiteral
from .placeholder.resolve import resolve_param
from .placeholder.parse import parse_param
from .source import Source, SourceIndex, SourceSpan

SubstitutionMap: TypeAlias = Mapping[ParamName, ValueLiteral]
ParamSet: TypeAlias = Mapping[ParamName, ParamResolved]
SequenceElement: TypeAlias = SourceSpan | ParamName
TemplateSequence: TypeAlias = tuple[SequenceElement, ...]

@dataclass(frozen=True, slots=True)
class Template:
    source: Source
    params: ParamSet
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

            params[par.name] = par
            elements.append(par.name)

        _append_span(last, SourceIndex(len(source.contents)))

        return Template(
            source=source,
            params=MappingProxyType(params),
            sequence=tuple(elements)
        )

    def defaults(self: Template) -> SubstitutionMap:
        subs = {}
        for name, param in self.params.items():
            subs[name] = param.default
        return subs

    def render_to_object(self, subs: SubstitutionMap) -> Render:
        # TODO:
        return Render("", self, subs)

    def render_to_file(self, _: SubstitutionMap, ) -> None:
        # TODO:
        pass

@dataclass(frozen=True, slots=True)
class Render:
    text: str
    input: Template
    subs: SubstitutionMap
