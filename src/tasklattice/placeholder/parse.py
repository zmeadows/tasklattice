from ast import literal_eval
from typing import Any

from lark import Lark, ParseTree, Token, Transformer

from .grammar import TL_GRAMMAR
from .model import Identifier, Literal, ParamUnresolved
from .source import Placeholder


class _TLTransformer(Transformer[Token, ParamUnresolved]):
    def __init__(self, ph: Placeholder):
        super().__init__()
        self._ph = ph

    def start(self, items: list[Any]) -> ParamUnresolved:
        pu = ParamUnresolved(items[0].value, items[1])

        for tag, contents in items[2:]:
            if tag.value == "type":
                pu.type_raw = contents
            elif tag.value == "domain":
                pu.domain_raw = contents
            elif tag.value == "desc":
                pu.desc = contents
            else:
                raise ValueError(f"Unknown placeholder tag: {tag.value}")

        return pu

    def pair(self, items: list[Any]) -> tuple[str, Any]:
        return items[0], items[1]

    def number(self, items: list[Token]) -> float:
        return float(items[0].value)

    def string(self, items: list[Token]) -> str:
        # Unquote the ESCAPED_STRING (e.g. '"hello"' → 'hello')
        return str(literal_eval(items[0].value))

    def lopen(self, _: list[Token]) -> str:
        return "("

    def lclosed(self, _: list[Token]) -> str:
        return "["

    def ropen(self, _: list[Token]) -> str:
        return ")"

    def rclosed(self, _: list[Token]) -> str:
        return "]"

    def true(self, _: list[Token]) -> bool:
        return True

    def false(self, _: list[Token]) -> bool:
        return False

    def interval(self, items: list[Any]) -> list[Any]:
        return items

    def set(self, items: list[Any]) -> set[Literal]:
        return set(items)

    def identifier(self, items: list[Token]) -> Identifier:
        return Identifier(items[0].value)

_PARSER = Lark(TL_GRAMMAR, start="start", parser="lalr", propagate_positions=True)

def parse_param(ph: Placeholder) -> ParamUnresolved:
    tree: ParseTree = _PARSER.parse(ph.text)
    return _TLTransformer(ph).transform(tree)
