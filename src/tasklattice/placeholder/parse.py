from ast import literal_eval
from typing import Any

from lark import Lark, ParseTree, Token, Transformer

from .grammar import TL_GRAMMAR
from .model import Identifier, Literal, Number, ParamUnresolved
from .source import Placeholder


def _is_integer_string(s: str) -> bool:
    if s.startswith(("+", "-")):
        return s[1:].isdigit()
    return s.isdigit()

class _TLTransformer(Transformer[Token, ParamUnresolved]):
    def __init__(self, ph: Placeholder):
        super().__init__()
        self._ph = ph

    def start(self, items: list[Any]) -> ParamUnresolved:
        meta_pairs = dict((k.value, v) for (k,v) in items[2:])

        ALLOWED_META_LABELS = set(["type", "domain", "desc"])

        unknown_meta_labels = set(meta_pairs.keys()) - ALLOWED_META_LABELS

        if unknown_meta_labels:
            raise ValueError(f"Unknown placeholder meta identifiers: {unknown_meta_labels}") 

        return ParamUnresolved(
            name=items[0].value,
            default=items[1],
            type_raw=meta_pairs.get("type", None),
            domain_raw=meta_pairs.get("domain", None),
            description=meta_pairs.get("desc", None)
        )

    def pair(self, items: list[Any]) -> tuple[str, Any]:
        return items[0], items[1]

    def number(self, items: list[Token]) -> Number:
        num_str = items[0].value

        if _is_integer_string(num_str):
            return int(num_str)
        else:
            return float(num_str)

    def string(self, items: list[Token]) -> str:
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
