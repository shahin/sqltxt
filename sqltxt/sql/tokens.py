
from pyparsing import (
    Keyword,
    CaselessKeyword,
    CaselessLiteral,
    oneOf,
    Word,
    Optional,
    Suppress,
    delimitedList,
    MatchFirst,
    Combine,
    quotedString,
    alphas,
    alphanums,
    nums,
    printables
)

# keywords
(UNION, ALL, AND, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, 
 CROSS, RIGHT, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, GROUP, BY,
 HAVING, ORDER, BY, LIMIT, OFFSET, TABLESAMPLE) =  map(CaselessKeyword, """UNION, ALL, AND, INTERSECT, 
 EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, CROSS, RIGHT, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, 
 DISTINCT, FROM, WHERE, GROUP, BY, HAVING, ORDER, BY, LIMIT, OFFSET, TABLESAMPLE""".replace(",","").split())
(CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
 COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE, 
 CURRENT_TIMESTAMP) = map(CaselessKeyword, """CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, 
 END, CASE, WHEN, THEN, EXISTS, COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, 
 CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP""".replace(",","").split())
keyword = MatchFirst((UNION, ALL, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, 
 CROSS, RIGHT, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, GROUP, BY,
 HAVING, ORDER, BY, LIMIT, OFFSET, CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
 COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE, 
 CURRENT_TIMESTAMP, TABLESAMPLE))

select_tok = Keyword('select', caseless=True)
from_tok = Keyword('from', caseless=True) 

# for parsing select-from statements
idr = ~keyword + Word(alphas + '*', alphanums + '_/-.*').setName('identifier')

table_path = Word(''.join([c for c in printables if c not in "?"])).setResultsName('path')
table_alias = idr.setResultsName('alias')
table_idr = table_path + Optional(Optional(Suppress('as')) + table_alias)

column_idr = delimitedList(idr, '.', combine=True)
aggregate_function = Combine(Keyword('count').setResultsName('function_name') + '(' + column_idr.setResultsName('function_argument') + ')')

# for parsing where statements
and_ = Keyword('and', caseless=True)
or_ = Keyword('or', caseless=True)
in_ = Keyword('in', caseless=True)

E = CaselessLiteral('E')
binary_op = oneOf('= != < > >= <= eq ne lt le gt ge', caseless=True)
arith_sign = Word('+-', exact=1)

real_num = Combine( 
    Optional(arith_sign) + 
    ( Word(nums) + '.' + Optional( Word(nums) ) | ( '.' + Word(nums) ) ) + 
    Optional( E + Optional(arith_sign) + Word(nums) ) 
    )

int_num = Combine(
    Optional(arith_sign) + Word(nums) + Optional( E + Optional('+') + Word(nums) )
    )

column_val = real_num | int_num | quotedString | column_idr
