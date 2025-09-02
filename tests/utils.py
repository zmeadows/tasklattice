from __future__ import annotations

#from typing import Iterable

def tl(body: str) -> str:
    """Wrap a TL body in full placeholder delimiters."""
    result = "{{" + body + "}}"
    return result

# def to_set_literal(vals: Iterable[object]) -> str:
#     return "{" + ",".join(str(v) for v in vals) + "}"
