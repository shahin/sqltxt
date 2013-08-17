from sql_tokenizer import SqlTokenizer
from table import Table
from copy import deepcopy

class Query:

  def __init__(self, column_names, from_clauses, where_clauses):

    self.from_clauses = from_clauses
    self.column_names = column_names
    self.tables = {}
    self.where_clauses = where_clauses

    for from_clause in self.from_clauses:
      if 'join' in from_clause[0]:
        return None

  def generate_table(self):

    # instantiate a Table for each table listed in from-clauses
    for idx, from_clause in enumerate(self.from_clauses):
      if len(from_clause) == 1:
        table_name = from_clause[0]
      else:
        table_name = from_clause[1]
      self.tables[table_name] = Table(table_name)
    
    n_from_clauses = len(self.from_clauses)

    if n_from_clauses > 1:
      left_subquery = deepcopy(self)
      del left_subquery.from_clauses[:-1]
      right_subquery = deepcopy(self)
      del right_subquery.from_clauses[-1]
      joined_table = self.join(
        left_subquery.generate_table(),
        right_subquery.generate_table(),
        self._qualify_column_names(right_subquery.from_clauses[0][3])
        )
      return joined_table

    elif n_from_clauses == 1:
      if len(self.from_clauses[0]) == 1:
        table_name = self.from_clauses[0][0]
      else:
        table_name = self.from_clauses[0][1]

      result_table = Table(table_name)

      if self.where_clauses:
        where_conditions = self._normalize_sql_boolean_operators(self.where_clauses)
        result_table.select_subset(where_conditions)
      
      #if self.order_by is not None:
      #  table.sort()
      result_table.order_columns(self.column_names, drop_other_columns=True)
      return result_table
    
    return None

  def join(self, left_subquery_table, right_subquery_table, join_conditions):

    # TODO: check sortedness of both tables

    # left and right vars are always on the respoective site of the operator
    # only equality joins supported here
    # only 'and' joins supported here
    left_condition_vars = [condition[0] for condition in join_conditions]
    right_condition_vars = [condition[2] for condition in join_conditions]

    left_indices = []
    right_indices = []
    for condition in join_conditions:
      var1_table_name, var1_name = condition[0].split('.')

      if var1_table_name == left_subquery_table.name:
        left_indices.append(left_subquery_table.column_idxs[var1_name])
      elif var1_table_name == right_subquery_table.name:
        right_indices.append(right_subquery_table.column_idxs[var1_name])

      var2_table_name, var2_name = condition[2].split('.')
      if var2_table_name == left_subquery_table.name:
        left_indices.append(left_subquery_table.column_idxs[var2_name])
      elif var2_table_name == right_subquery_table.name:
        right_indices.append(right_subquery_table.column_idxs[var2_name])

      left_indices_arg = ','.join(left_indices)
      right_indices_arg = ','.join(right_indices)
      
    left_filename = left_subquery_table.name + '.out'
    right_filename = right_subquery_table.name + '.out'

    join_cmd = "join -t, -1 {0} -2 {1} {2} {3}".format(
      left_indices_arg, right_indices_art, left_filename, right_filename)

    cmd = ' '.join(
      left_subquery_table.to_file_cmd(left_filename) + 
      right_subquery_table.to_file_cmd(right_filename) + 
      join_cmd)

    return cmd

  def _qualify_column_names(unqualified_column_names, tables):
    """Return a dict of lists of qualified column names for the specified columns.

    ['col_a', 'col_b', 'col_dne'], [<Table 'table_a'>, <Table 'table_b'>] -> 
      {'col_a': ['table_a.col_a'], 'col_b': ['table_a.col_b', 'table_b.col_b'], 'col_dne': []}
    """

    qualified_names = {}
    
    for col_name in unqualified_column_names:
      names_for_this_col = []
      for table in tables:
        if col_name in table.column_names:
          names_for_this_col.append(table.name + '.' + col_name)
      qualified_names[col_name] = names_for_this_col

    return qualified_names


  def _normalize_sql_boolean_operators(self, sql_where_clauses):

    sql_to_bool_operators = {
        '=': '==',
        'eq': '==',
        'ne': '!=',
        'ge': '>=',
        'gt': '>',
        'le': '<=',
        'lt': '<'
        }

    bool_where_clauses = []

    # translate SQL-specific boolean operators to the tokens that normal languages use
    if len(sql_where_clauses) > 0:
      for clause in sql_where_clauses:
        bool_clause = [ sql_to_bool_operators.get(token, token) for token in clause ]
        bool_where_clauses.append(bool_clause)

    return bool_where_clauses
