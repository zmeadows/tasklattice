TL_GRAMMAR = r"""
start: "TL" identifier "=" literal ("," pair)*

pair: identifier ":" meta_value

identifier: CNAME

number: SIGNED_NUMBER

string: /'[^']*'/

?literal: string
        | number
        | boolean

boolean: "true"                -> true
       | "false"               -> false

?meta_value: literal
           | interval
           | set
           | identifier

interval: lpar number "," number rpar

lpar: "(" -> lopen
    | "[" -> lclosed

rpar: ")" -> ropen
    | "]" -> rclosed

set: "{" [literal ("," literal)*] "}"

%import common.CNAME
%import common.SIGNED_NUMBER
%import common.WS
%ignore WS
"""
