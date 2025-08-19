# import math
# from typing import Any
# 
# from .model import Literal, ParamResolved, ParamUnresolved
# 
# 
# def _int_to_float_checked(x: int) -> float:
#     f = float(x)
# 
#     if math.isinf(f):
#         raise OverflowError(f"Integer {x} too large to represent as float")
# 
#     if int(f) != x:
#         # This means the float couldn't exactly store the integer
#         print(f"Warning: integer {x} not exactly representable as float ({f})")
# 
#     return f
# 
# def _resolve_default(default: Literal, user_type: type[Literal] | None):
#     if user_type is None and type(default) is int:
#         return _int_to_float_checked(default)
#     elif user_type is None or type(default) is user_type:
#         return default
#     elif type(default) is int and user_type is float:
#         return _int_to_float_checked(default)
#     elif type(default) is not user_type:
#         raise ValueError(
#             f"""Unable to represent user-specified default ({default})
#                 as user-specified type ({user_type})"""
#         )
# 
#     return default
# 
# def _resolve_domain(domain_raw: Any, user_type: type[Literal] | None) -> None:
#     # TODO:
#     return None
# 
# def resolve_parameter(pu: ParamUnresolved) -> ParamResolved:
#     ALLOWED_USER_TYPES = {
#         "str" : str,
#         "float" : float,
#         "int" : int,
#         "bool" : bool,
#     }
# 
# 
#     user_type = None
#     if pu.py_type_raw is not None:
#         user_type = ALLOWED_USER_TYPES.get(pu.py_type_raw, None)
# 
#         if user_type is None:
#             raise RuntimeError(f"Unknown user-specified type label: {pu.py_type_raw}")
# 
#     return ParamResolved(
#         name=pu.name,
#         default=_resolve_default(pu.default, user_type),
#         domain=_resolve_domain(pu.domain_raw, user_type),
#         description=None
#     )
# 
