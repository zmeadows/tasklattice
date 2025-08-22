TL_GRAMMAR = r"""
start: OPEN_PH TL_KW param CLOSE_PH

param: IDENTIFIER "=" literal ("," pair)*

pair: domain_pair
    | type_pair
    | description_pair

domain_pair: "domain" ":" (interval | set)
type_pair: "type" ":" IDENTIFIER
description_pair: "desc" ":" STRING

?literal: STRING | number | boolean
?number: INT | FLOAT

INT: /[+-]?\d+/
FLOAT: /[+-]?(?:\d+\.\d*|\.\d+)(?:[eE][+-]?\d+)?|[+-]?\d+(?:[eE][+-]?\d+)/

DQUOTE_STRING: /"(\\.|[^"\\])*"/
SQUOTE_STRING: /'(\\.|[^'\\])*'/
STRING: DQUOTE_STRING | SQUOTE_STRING

BOOLEAN_TRUE: /(?i:true)/
BOOLEAN_FALSE: /(?i:false)/
boolean: BOOLEAN_TRUE -> true
       | BOOLEAN_FALSE -> false

interval: LPAR number "," number RPAR
LPAR: "(" | "["
RPAR: ")" | "]"

set: "{" [set_elem ("," set_elem)*] "}"
set_elem: number | STRING

IDENTIFIER: /[A-Za-z_][A-Za-z0-9_]*/

OPEN_PH: "{{"
CLOSE_PH: "}}"
TL_KW: "TL"

%import common.WS
%ignore WS
"""
