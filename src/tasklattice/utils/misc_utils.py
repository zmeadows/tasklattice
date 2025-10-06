import sys


def here() -> str:
    fr = sys._getframe(1)  # caller's frame
    func = fr.f_code.co_name
    loc = fr.f_locals
    if "self" in loc:  # instance method
        return f"{loc['self'].__class__.__qualname__}.{func}"
    if "cls" in loc:  # classmethod
        return f"{loc['cls'].__qualname__}.{func}"
    return func  # staticmethod or free function
