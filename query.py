from sql_tokenizer import SqlTokenizer
from table import Table

class Query:

  def __init__(self, sql_text):
    self.sql_text = sql_text
    self.tables = {}

    t = SqlTokenizer()
    self.tokens = t.parse(self.sql_text)
    print(self.tokens.from_clauses.asList())

    self.column_names = self.tokens.column_names.asList()
    self.qualified_names = self._qualify_column_names()

    #import pdb; pdb.set_trace()

    # instantiate a Table for each table listed in the from-expression
    for idx, from_clause in enumerate(self.tokens.from_clauses.asList()):
      if idx == 0:
        table_name = from_clause[0]
      else:
        table_name = from_clause[1]
      self.tables[table_name] = Table(table_name)


  def generate_table(self):

    commands = []
    table = list(self.tables.values())[0]
    select_columns = ','.join(['$' + str(table.column_idxs[c]+1) for c in self.column_names])
    select_conditions = self._sql_bools_to_awk_bools(self.tokens)
    awk_cmd = "awk -F',' 'OFS=\",\" {{ if ({0}) {{ print {1} }} }}'".format(select_conditions,select_columns)
    commands.append(awk_cmd)

    command_str = ' | '.join(commands) + ' < ' + table.name + '.txt'

    return Table('result_{0}'.format(id(self)), cmd=command_str, column_names = self.column_names)


  def _qualify_column_names(self):
    """Return a list of lists of qualified column names for the select columns."""

    qualified_names = {}
    
    for col_name in self.column_names:
      names_for_this_col = []
      for table_name in self.tables.keys():
        if col_name in self.tables[table_name].column_names:
          names_for_this_col.append(table_name + '.' + col_name)
      qualified_names[col_name] = names_for_this_col

    return qualified_names


  def _sql_bools_to_awk_bools(self, parsed):

    sql_to_awk_operators = {
        '=': '==',
        'eq': '==',
        'ne': '!=',
        'ge': '>=',
        'gt': '>',
        'le': '<=',
        'lt': '<'
        }

    table = list(self.tables.values())[0]

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
          expr_part = [ ('$' + str(table.column_idxs[token]+1) if token in table.column_idxs else token) for token in expr_part ]
          select_conditions += ' '.join(expr_part)

    return select_conditions
