from ast import literal_eval
from typing import Any

from lark import Lark, ParseTree, Token, Transformer, v_args

from .grammar import TL_GRAMMAR
from .model import (
    DomainIntervalUnresolved,
    DomainSetUnresolved,
    Number,
    ParamName,
    ParamUnresolved,
    Placeholder,
    SetLiteral,
    ValueLiteral,
)


class _TLTransformer(Transformer[Token, ParamUnresolved]):
    def __init__(self, ph: Placeholder):
        super().__init__()
        self._ph = ph

    @v_args(inline=True)
    def start(self, name: str, default: ValueLiteral, *meta: list[Any]) -> ParamUnresolved:
        ALLOWED_META_LABELS = set(["type", "domain", "desc"])

        meta_pairs = {}
        for key, value in meta:
            if key in meta_pairs:
                raise ValueError(f"duplicate key detected: {key}")
            elif key not in ALLOWED_META_LABELS:
                raise ValueError(f"Unknown placeholder meta identifier: {key}") 
            meta_pairs[key] = value

        return ParamUnresolved(
            name=ParamName(name),
            default=default,
            py_type=meta_pairs.get("type", None),
            domain=meta_pairs.get("domain", None),
            description=meta_pairs.get("desc", None),
        )

    def INT(self, tok: Token) -> int:
        return int(tok.value)

    def FLOAT(self, tok: Token) -> float:
        return float(tok.value)

    def STRING(self, tok: Token) -> str:
        return str(literal_eval(tok.value))

    def true(self, _: Token) -> bool:
        return True

    def false(self, _: Token) -> bool:
        return False

    def IDENTIFIER(self, tok: Token) -> str:
        return str(tok.value)

    def number(self, items: list[Number]) -> int | float:
        return items[0]

    def set_elem(self, items: list[SetLiteral]) -> Any:
        return items[0]

    def set(self, items: list[SetLiteral]) -> DomainSetUnresolved:
        return DomainSetUnresolved(items)

    def interval(self, items: list[Any]) -> DomainIntervalUnresolved:
        if len(items) == 5:
            lpar, lower, upper, rpar = items[0], items[1], items[3], items[4]
        else:
            lpar, lower, upper, rpar = items[0], items[1], items[2], items[3]

        return DomainIntervalUnresolved(
            lower=lower,
            upper=upper,
            lpar=lpar,
            rpar=rpar
        )

    def type_pair(self, items: list[Any]) -> tuple[str, str]:
        return ("type", items[0])

    def description_pair(self, items: list[Any]) -> tuple[str, str]:
        return ("desc", items[0])

    def domain_pair(self, items: list[Any]) -> tuple[str, Any]:
        return ("domain", items[0])

    def pair(self, kv: list[tuple[str, Any]]) -> tuple[str, Any]:
        return kv[0]

_PARSER = Lark(
    TL_GRAMMAR,
    start="start",
    parser="lalr",
    propagate_positions=True,
    lexer="contextual",
    cache=False,
)

def parse_param(ph: Placeholder) -> ParamUnresolved:
    tree: ParseTree = _PARSER.parse(ph.text)
    return _TLTransformer(ph).transform(tree)

def parse_param_str(ph_str: str) -> ParamUnresolved:
    ph = Placeholder.from_string(ph_str)
    tree: ParseTree = _PARSER.parse(ph.text)
    return _TLTransformer(ph).transform(tree)
