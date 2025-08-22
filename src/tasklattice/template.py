from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import NewType

from .placeholder.model import Literal, ParamName, ParamResolved
from .source import Source

SubstitutionMap = NewType("SubstitutionMap", Mapping[ParamName, Literal])

TemplateSequence = NewType("TemplateSequence", tuple[str | ParamName])

ParamSet = NewType("ParamSet", Mapping[ParamName, ParamResolved])

@dataclass(frozen=True, slots=True)
class TemplateInput:
    source: Source
    params: ParamSet
    sequence: TemplateSequence

    @staticmethod
    def from_source(source: Source) -> TemplateInput:
        # TODO:
        return TemplateInput(
            source,
            ParamSet(MappingProxyType({})),
            TemplateSequence(("asdf",))
        )

    def render(self, subs: SubstitutionMap) -> RenderedInput:
        # TODO:
        return RenderedInput("", self, subs)

@dataclass(frozen=True, slots=True)
class RenderedInput:
    text: str
    input: TemplateInput
    subs: SubstitutionMap
