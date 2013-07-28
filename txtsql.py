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

class Interpreter:
  
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
    from_relation = Group( table_idr) + ZeroOrMore( 
      Group( 
        join_type.setResultsName('join_type') + table_idr.setResultsName('right_relation') + on_ + where_cond.setResultsName('join_condition') 
      )).setResultsName('joins')

    select_stmt << (
        select_tok + 
        ( '*' | column_idr_list ).setResultsName('columns') +
        from_tok +
        from_relation.setResultsName('tables') +
        Optional( Group( CaselessLiteral("where") + where_expr ), "" ).setResultsName("where") +
        StringEnd()
        )

    return select_stmt

  def parse(self, sql_str):
    """Return the parsed string as a pyparsing.ParseResults-like object"""
    return self.grammar.parseString(sql_str)

  def translate(self,parsed):
    """Return a shell script implementing the parsed query"""

    sql_to_awk_operators = {
        '=': '==',
        'eq': '==',
        'ne': '!=',
        'ge': '>=',
        'gt': '>',
        'le': '<=',
        'lt': '<'
        }

    print(parsed.tables)
    with open(parsed.tables[0] + '.txt') as table_file:
      head = table_file.readline()

    # map column names to their ordinal column numbers
    file_columns = head.strip().upper().split(',')
    select_columns = parsed.columns.asList()

    self.table_column_idxs = { c.upper():i for (i,c) in enumerate(file_columns) }
    print(self.table_column_idxs)

    # check that all selected columns are on the file
    for c in select_columns:
      if c not in file_columns:
        raise KeyError('Column {0} does not exist on file {1}.'.format(c, parsed.tables[0]))

    commands = []

    # translate SQL boolean conditions to awk syntax
    select_conditions = '1'
    where_tokens = parsed.where.asList()[0]
    if len(where_tokens) > 0:
      select_conditions = ''
      for expr_part in where_tokens[1:]:
        if expr_part == 'and':
          select_conditions += ' && '
        elif expr_part == 'or':
          select_conditions += ' || '
        else:
          expr_part = [ sql_to_awk_operators.get(token, token) for token in expr_part ]
          expr_part = [ ('$' + str(column_idxs[token]+1) if token in column_idxs else token) for token in expr_part ]
          select_conditions += ' '.join(expr_part)


    select_columns = ','.join(['$' + str(column_idxs[c]+1) for c in select_columns])
    awk_cmd = "awk -F',' 'OFS=\",\" {{ if ({0}) {{ print {1} }} }}'".format(select_conditions,select_columns)
    commands.append(awk_cmd)

    command_str = ' | '.join(commands) + ' < ' + parsed.tables[0] + '.txt'
    print(command_str)
    print(subprocess.check_output(command_str, shell=True).decode('UTF-8'))


  def qualify_column_names(self,parsed):
    """Given lists of names and table files, return a list of lists of qualified names."""

    headers = {}
    tables = [ (table_expr[0] if len(table_expr) == 1 else table_expr[1]) for table_expr in parsed.tables.asList() ];
    print(tables)

    for table in tables:
      with open(table + '.txt') as table_file:
        headers[table] = table_file.readline().strip().split(',')

    select_columns = parsed.columns.asList()
    qualified_names = {}
    print(headers)
    
    for col_name in select_columns:
      names_for_this_col = []
      for table_name in tables:
        if col_name in headers[table_name]:
          names_for_this_col.append(table_name + '.' + col_name)
      qualified_names[col_name] = names_for_this_col

    return qualified_names

  def translate_joins(self,parsed):
    """Given a two table names and a join condition with qualified column names, return a CLI join"""

    left_table = 'relation'
    right_table = 'relation2'

    return ''



if __name__ == '__main__':

  import argparse

  parser = argparse.ArgumentParser(description='Translate SQL to standard command line tools.')
  parser.add_argument('sql', metavar='SQL')

  args = parser.parse_args()
  sql_str = args.sql

  i = Interpreter();
  parsed = i.parse(sql_str)
  print(parsed)
  i.translate(parsed)
  
