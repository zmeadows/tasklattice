from tasklattice.placeholder.grammar import TL_GRAMMAR

from lark.load_grammar import load_grammar

def test_grammar_loads() -> None:
<<<<<<< HEAD
    load_grammar(TL_GRAMMAR, None, (), False)
=======
    load_grammar(TL_GRAMMAR, None, (), False) # type: ignore
>>>>>>> 3e29e27 (added grammar.py and associated lark grammar loading test)



