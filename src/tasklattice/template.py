from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TypeAlias

from .placeholder.model import PLACEHOLDER_RE, Literal, ParamName, ParamResolved, Placeholder
from .source import Source, SourceSpan

SubstitutionMap: TypeAlias = Mapping[ParamName, Literal]
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
        last = 0

        def _append_span(start: int, end: int) -> None:
            nonlocal elements
            if end > start:
                elements.append(SourceSpan(start, end))

        for match in PLACEHOLDER_RE.finditer(source.contents):
            ph = Placeholder.from_match(source, match)

            _append_span(ph.span_outer.start, last)

            # TODO:
            # pr: ParamResolved = ...
            # params[pr.name] = pr
            elements.append(ParamName("TODO"))

            last = ph.span_outer.end

        _append_span(last, len(source.contents))

        return Template(
            source=source,
            params=MappingProxyType(params),
            sequence=tuple(elements)
        )

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
