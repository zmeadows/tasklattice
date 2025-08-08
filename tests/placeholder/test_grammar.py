from tasklattice.placeholder.grammar import TL_GRAMMAR

from lark.load_grammar import load_grammar

def test_grammar_loads() -> None:
    load_grammar(TL_GRAMMAR, None, (), False) # type: ignore



