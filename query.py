from sql_tokenizer import SqlTokenizer
from table import Table
from copy import deepcopy

class Query:

  def __init__(self, from_clauses, where_clauses, column_names = None):

    self.from_clauses = from_clauses
    self.where_clauses = where_clauses
    self.column_names = column_names

    self.missing_select_columns = None

    self.left_table = None
    self.right_table = None

  def generate_table(self):

    # instantiate the left-most Table in all the where clauses
    first_from_clause_tokens = self.from_clauses[0]
    if len(first_from_clause_tokens) > 1:
      # first where clause is a join clause
      self.left_table = Table.from_filename(first_from_clause_tokens[1])
    else:
      # first where clause is not a join clause
      self.left_table = Table.from_filename(first_from_clause_tokens[0])

    if len(self.from_clauses) > 1:
      # instantiate the right Table as the result of a Query on all tables other than
      # the left-most

      right_subquery = Query(self.from_clauses[1:], [], self.column_names)
      self.right_table = right_subquery.generate_table()

      if right_subquery.missing_select_columns:
        # we are still missing any missing columns we don't find in the left table
        self.missing_select_columns = [
          col_name for col_name in right_subquery.missing_select_columns
          if col_name not in self.left_table.column_names
          ]
        
      joined_table = self.join(right_subquery.from_clauses[0][3:])

      if self.where_clauses:
        where_conditions = self._normalize_sql_boolean_operators(self.where_clauses)
        joined_table.select_subset(where_conditions)
      
      return joined_table

    else:

      self.missing_select_columns = [col_name for col_name in self.column_names
        if col_name not in self.left_table.column_names]

      if self.where_clauses:
        where_conditions = self._normalize_sql_boolean_operators(self.where_clauses)
        self.left_table.select_subset(where_conditions)

      self.left_table.order_columns(
          [col for col in self.column_names if col not in self.missing_select_columns], 
          drop_other_columns=True)

      return self.left_table

  def join(self, join_conditions):

    left_indices, right_indices = self._get_join_indices(join_conditions)

    left_indices_arg = ','.join([str(li + 1) for li in left_indices])
    right_indices_arg = ','.join([str(ri + 1) for ri in right_indices])

    if not self.left_table.is_sorted_by(left_indices):
      self.left_table.sort([self.left_table.column_names[i] for i in left_indices])

    if not self.right_table.is_sorted_by(right_indices):
      self.right_table.sort([self.right_table.column_names[i] for i in right_indices])

    # join the data
    join_cmd = "join -t, -1 {0} -2 {1} <({2}) <({3})".format(
      left_indices_arg, right_indices_arg, 
      self.left_table.get_cmd_str(), self.right_table.get_cmd_str())

    join_column_names = self._join_column_names(left_indices, right_indices)

    join_result_table = Table.from_cmd(
      name = 'join_result',
      cmd = join_cmd,
      column_names = join_column_names
      )

    return join_result_table

  def _get_join_indices(self, join_conditions):

    # only equality joins supported here
    # only 'and' joins supported here
    left_indices = []
    right_indices = []
    for condition in join_conditions:

      join_vars = (condition[0], condition[2])

      for join_var in join_vars:

        table_name, var_name = join_var.split('.')
        if table_name == self.left_table.name:
          left_indices.append(self.left_table.column_idxs[var_name])
        elif table_name == self.right_table.name:
          right_indices.append(self.right_table.column_idxs[var_name])

    return left_indices, right_indices

  def _join_column_names(self, left_indices, right_indices):

    n_columns_left = len(self.left_table.column_names)
    n_columns_right = len(self.right_table.column_names)

    join_column_names = [self.left_table.column_names[i] for i in left_indices]
    nonjoin_column_names = [self.left_table.column_names[i] for i in range(n_columns_left)
      if i not in left_indices]
    nonjoin_column_names += [self.right_table.column_names[i] for i in range(n_columns_right)
      if i not in right_indices]

    join_result_column_names = join_column_names + nonjoin_column_names

    return join_result_column_names
  
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
