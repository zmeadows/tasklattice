from lark.load_grammar import load_grammar

from tasklattice.placeholder.grammar import TL_GRAMMAR


def test_grammar_loads() -> None:
    load_grammar(TL_GRAMMAR, None, (), False)  # type: ignore
