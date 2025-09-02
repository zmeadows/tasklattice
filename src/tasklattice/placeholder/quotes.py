from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from tasklattice.source import SourceIndex, SourceSpan

QuoteType = Literal["single", "double"]

@dataclass(frozen=True, slots=True)
class QuoteContext:
    style: QuoteType
    left_index: SourceIndex
    right_index: SourceIndex

    @property
    def exterior(self) -> SourceSpan:
        return SourceSpan(self.left_index, self.right_index + 1)

    @property
    def interior(self) -> SourceSpan:
        return SourceSpan(self.left_index + 1, self.right_index)

