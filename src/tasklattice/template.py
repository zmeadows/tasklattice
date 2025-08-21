from collections.abc import Mapping
from dataclasses import dataclass
from typing import NewType

from .placeholder.model import Literal, ParamName, ParamResolved
from .source import Source

SubstitutionMap = NewType("SubstitutionMap", Mapping[ParamName, Literal])

TemplateSequence = NewType("TemplateSequence", list[str | ParamName])

@dataclass(frozen=True, slots=True)
class TemplateInput:
    source: Source
    params: dict[ParamName, ParamResolved]


@dataclass(frozen=True, slots=True)
class RenderedInput:
    text: str
    source: Source
    subs: SubstitutionMap

def _build_sequence(source: Source) -> TemplateSequence:
    # TODO: reduce adjacent 'str'
    return TemplateSequence([])

class InputRenderer:
    def __init__(self, source: Source):
        self._source: Source = source
        self._sequence: TemplateSequence = _build_sequence(self._source)

    def render(self, subs: SubstitutionMap) -> RenderedInput:
        return RenderedInput("", self._source, subs)



