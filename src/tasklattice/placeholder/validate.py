#from typing import Any

#from .model import Domain, DomainSet, Number, ParamResolved, ParamUnresolved


# def validate_parameter(pu: ParamUnresolved) -> ParamResolved:
#     pr = ParamResolved(pu.name, pu.default)
# 
#     ALLOWED_USER_TYPES = set(["str", "int", "float", "bool"])
# 
#     if pu.type_raw is not None and pu.type_raw not in ALLOWED_USER_TYPES:
#         #TODO: expand to custom error type
#         raise RuntimeError("Invalid Type: " + pu.type_raw)
# 
#     user_type = globals()[pu.type_raw]
# 
#     elif pu.type_raw is None and isinstance(pu.default, int):
#         # if user doesn't specify that values are 'int' type, let them be floats
#         pr.default = float(pu.value)
# 
#     if pu.domain_raw is not None:
#         pr.domain = validate_domain(pu.domain_raw, user_type)
# 
#     return pr
# 
# def validate_domain(_: list[Any], ptype: type) -> Domain:
#     #TODO:
#     return DomainSet(set())
# 
# def infer_number_type(num: Number) -> Number:
#     #TODO:
#     return num
