import subprocess
from pyparsing import (
  Forward, 
  Keyword,
  Word,
  Group,

  delimitedList,
  Upcase,
  oneOf,
  ZeroOrMore,
  Combine,
  Optional,
  StringEnd,

  CaselessLiteral,
  quotedString,
  alphas,
  alphanums,
  nums
  )

class SqlTokenizer:
  
  def __init__(self):
    self.grammar = self._build_grammar()

  def _build_grammar(self):
    """Return the grammar as a pyparsing.Forward-like object"""

    select_tok = Keyword('select', caseless=True)
    from_tok = Keyword('from', caseless=True) 

    # for parsing select-from statements
    idr = Word(alphas, alphanums + '_$').setName('identifier')
    column_idr = Upcase(delimitedList(idr, '.', combine=True))
    column_idr_list = Group(delimitedList(column_idr))
    table_idr = Upcase(delimitedList(idr, '.', combine=True))
    table_idr_list = Group(delimitedList(table_idr))

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
      ( column_idr + binary_op + column_val ) |
      ( column_idr + in_ + "(" + delimitedList( column_val ) + ")" ) |
      ( column_idr + in_ + "(" + select_stmt + ")" ) |
      ( "(" + where_expr + ")" )
      )

    join_cond = where_cond

    where_expr << where_cond + ZeroOrMore( (and_ | or_) + where_expr )

    on_ = Keyword('on', caseless=True)
    join_type = Group(
        ( Optional('inner') + 'join' ) |
        ( oneOf('left right full') + Optional('outer') + 'join' )
        )

    from_expr = Forward()
    from_relation = Group( table_idr ) + ZeroOrMore( 
      Group( 
        join_type.setResultsName('join_type') + table_idr.setResultsName('right_relation') + on_ + where_cond.setResultsName('join_condition') 
      )).setResultsName('joins')

    select_stmt << (
        select_tok + 
        ( '*' | column_idr_list ).setResultsName('column_names') +
        from_tok +
        from_relation.setResultsName('from_clauses') +
        Optional( Group( CaselessLiteral("where") + where_expr ), "" ).setResultsName("where") +
        StringEnd()
        )

    return select_stmt

  def parse(self, sql_str):
    """Return the parsed string as a pyparsing.ParseResults-like object"""
    return self.grammar.parseString(sql_str)



