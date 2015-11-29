import subprocess
from pyparsing import (
  Forward, 
  Keyword,
  Word,
  Group,
  CaselessKeyword,
  MatchFirst,

  delimitedList,
  Upcase,
  oneOf,
  ZeroOrMore,
  Combine,
  Optional,
  StringEnd,
  Suppress,

  CaselessLiteral,
  quotedString,
  alphas,
  alphanums,
  nums,
  printables
  )

# keywords
(UNION, ALL, AND, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, 
 CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, GROUP, BY,
 HAVING, ORDER, BY, LIMIT, OFFSET) =  map(CaselessKeyword, """UNION, ALL, AND, INTERSECT, 
 EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, 
 DISTINCT, FROM, WHERE, GROUP, BY, HAVING, ORDER, BY, LIMIT, OFFSET""".replace(",","").split())
(CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
 COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE, 
 CURRENT_TIMESTAMP) = map(CaselessKeyword, """CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, 
 END, CASE, WHEN, THEN, EXISTS, COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, 
 CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP""".replace(",","").split())
keyword = MatchFirst((UNION, ALL, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, 
 CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, GROUP, BY,
 HAVING, ORDER, BY, LIMIT, OFFSET, CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
 COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE, 
 CURRENT_TIMESTAMP))

select_tok = Keyword('select', caseless=True)
from_tok = Keyword('from', caseless=True) 

# for parsing select-from statements
idr = ~keyword + Word(alphas + '*', alphanums + '_*').setName('identifier')

table_path = Word(''.join([c for c in printables if c not in "?"])).setResultsName('path')
table_alias = idr.setResultsName('alias')
table_idr = table_path + Optional(Optional(Suppress('as')) + table_alias)

column_idr = delimitedList(idr, '.', combine=True)
aggregate_function = Combine(Keyword('count') + '(' + column_idr + ')')
column_list = Group(delimitedList((column_idr ^ aggregate_function.setResultsName('aggregate_functions', listAllMatches=True))))

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

select_stmt = Forward()
where_expr = Forward()
where_cond = Group(
  ( column_idr.setResultsName('left_operand') + 
    binary_op.setResultsName('operator') + 
    column_val.setResultsName('right_operand') 
    ).setResultsName('binary_condition') |
  ( column_idr.setResultsName('left_operand') + 
    in_ + 
    Suppress("(") + delimitedList( column_val ).setResultsName('right_operand') + Suppress(")") 
    ).setResultsName('in_list_condition') |
  ( column_idr.setResultsName('left_operand') + 
    in_ + 
    Suppress("(") + select_stmt.setResultsName('right_operand') + Suppress(")")
    ).setResultsName('in_query_condition') |
  ( Suppress("(") + where_expr + Suppress(")") )
  )

group_by_expr = Group(column_idr + ZeroOrMore( "," + column_idr ))

where_expr << where_cond + ZeroOrMore( (and_ | or_) + where_expr )

on_ = Keyword('on', caseless=True)
join = ((Optional('inner') + 'join') | (oneOf('left right') + 'join' )
        ).setResultsName('join_type')

from_clause = table_idr.setResultsName('left_relation') + ZeroOrMore(Group(
    join + table_idr.setResultsName('right_relation') + on_ + where_cond.setResultsName('join_conditions') 
  )).setResultsName('joins', listAllMatches=True)

select_stmt << (
    select_tok + 
    ( column_list ).setResultsName('column_definitions') +
    from_tok +
    from_clause.setResultsName('from_clause') +
    Optional( CaselessLiteral("where") + where_expr.setResultsName("where_conditions") ) +
    Optional( CaselessLiteral("group by") + group_by_expr.setResultsName("group_by_column_names") ) + 
    StringEnd()
    )

