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
  Suppress,
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
    idr = Word(alphas + '*', alphanums + '_*').setName('identifier')
    column_idr = delimitedList(idr, '.', combine=True)
    aggregate_function = Combine(Keyword('count') + '(' + Group(delimitedList(column_idr)) + ')')

    column_list = delimitedList(
        Group(column_idr + ZeroOrMore(Suppress('as') + column_idr)) ^ 
        Group(aggregate_function + ZeroOrMore(Suppress('as') + column_idr)
          ).setResultsName('aggregate_functions', listAllMatches = True)
        )

    table_idr = Upcase(delimitedList(idr, '.', combine=True))
    table_idr_list = delimitedList(table_idr)

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
        "(" + delimitedList( column_val ).setResultsName('right_operand') + ")" 
        ).setResultsName('in_list_condition') |
      ( column_idr.setResultsName('left_operand') + 
        in_ + 
        "(" + select_stmt.setResultsName('right_operand') + ")" 
        ).setResultsName('in_query_condition') |
      ( "(" + where_expr + ")" )
      )

    group_by_expr = Group(column_idr + ZeroOrMore( "," + column_idr ))

    join_cond = where_cond

    where_expr << where_cond + ZeroOrMore( (and_ | or_) + where_expr )

    on_ = Keyword('on', caseless=True)
    join = Group(
        ( Optional('inner').setResultsName('join_type') + 'join' ) |
        ( oneOf('left right').setResultsName('join_type') + 'join' )
        )

    from_expr = Forward()
    from_clause = Group( table_idr ) + ZeroOrMore( 
      Group( 
        join + table_idr.setResultsName('right_relation') + on_ + where_cond.setResultsName('join_condition') 
      )).setResultsName('joins')

    select_stmt << (
        select_tok + 
        ( column_list ).setResultsName('column_definitions') +
        from_tok +
        from_clause.setResultsName('from_clause') +
        Optional( CaselessLiteral("where") + where_expr.setResultsName("where_conditions") ) +
        Optional( CaselessLiteral("group by") + group_by_expr.setResultsName("group_by_column_names") ) + 
        StringEnd()
        )

    return select_stmt

  def parse(self, sql_str):
    """Return the parsed string as a pyparsing.ParseResults-like object"""
    return self.grammar.parseString(sql_str)



