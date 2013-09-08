from sql_tokenizer import SqlTokenizer
from table import Table
from copy import deepcopy

class Query:

  def __init__(self, from_clauses, where_clauses, column_names = None):

    self.from_clauses = from_clauses
    self.where_clauses = where_clauses
    self.column_names = column_names

    self.missing_select_columns = None

    self.tables = {}


  def generate_table(self):

    # instantiate the left-most Table in all the where clauses
    first_from_clause_tokens = self.from_clauses[0]
    if len(first_from_clause_tokens) > 1:
      # first where clause is a join clause
      left_table = Table.from_filename(first_from_clause_tokens[1])
    else:
      # first where clause is not a join clause
      left_table = Table.from_filename(first_from_clause_tokens[0])

    if len(self.from_clauses) > 1:
      # instantiate the right Table as the result of a Query on all tables other than
      # the left-most

      right_subquery = Query(self.from_clauses[1:], [], self.column_names)
      right_table = right_subquery.generate_table()

      if right_subquery.missing_select_columns:
        # we are still missing any missing columns we don't find in the left table
        self.missing_select_columns = [
          col_name for col_name in right_subquery.missing_select_columns
          if col_name not in left_table.column_names
          ]
        
      joined_table = self.join(
        left_table,
        right_table,
        self._qualify_column_names(right_table.from_clauses[0][3])
        )

      if self.where_clauses:
        where_conditions = self._normalize_sql_boolean_operators(self.where_clauses)
        joined_table.select_subset(where_conditions)
      
      return joined_table

    else:

      self.missing_select_columns = [col_name for col_name in self.column_names
        if col_name not in left_table.column_names]

      if self.where_clauses:
        where_conditions = self._normalize_sql_boolean_operators(self.where_clauses)
        left_table.select_subset(where_conditions)

      left_table.order_columns(self.column_names, drop_other_columns=True)

      return left_table
    

  def join(self, left_table, right_table, join_conditions):

    # left and right vars are always on the respoective site of the operator
    # only equality joins supported here
    # only 'and' joins supported here
    left_condition_vars = [condition[0] for condition in join_conditions]
    right_condition_vars = [condition[2] for condition in join_conditions]

    left_indices = []
    right_indices = []
    for condition in join_conditions:
      var1_table_name, var1_name = condition[0].split('.')

      if var1_table_name == left_table.name:
        left_indices.append(left_table.column_idxs[var1_name])
      elif var1_table_name == right_table.name:
        right_indices.append(right_table.column_idxs[var1_name])

      var2_table_name, var2_name = condition[2].split('.')
      if var2_table_name == left_table.name:
        left_indices.append(left_table.column_idxs[var2_name])
      elif var2_table_name == right_table.name:
        right_indices.append(right_table.column_idxs[var2_name])

      left_indices_arg = ','.join([str(li) for li in left_indices])
      right_indices_arg = ','.join([str(ri) for ri in right_indices])

    if not left_table._is_sorted(left_indices):
      left_table.sort([left_table.column_names[i-1] for i in left_indices])

    if not right_table._is_sorted(right_indices):
      right_table.sort([right_table.column_names[i-1] for i in right_indices])

    # join the data
    join_cmd = "join -t, -1 {0} -2 {1} <({2}) <({3})".format(
      left_indices_arg, right_indices_arg, 
      left_table.get_cmd_str(), right_table.get_cmd_str())

    join_column_names = self._join_column_names(
      left_table, right_table, left_indices, right_indices)

    join_result_table = Table.from_cmd(
      name = 'join_result',
      cmd = join_cmd,
      column_names = join_column_names
      )

    return join_result_table

  def _join_column_names(self, left_table, right_table, left_indices, right_indices):

    n_columns_left = len(left_table.column_names)
    n_columns_right = len(right_table.column_names)

    join_column_names = [left_table.column_names[i-1] for i in left_indices]
    nonjoin_column_names = [left_table.column_names[i] for i in range(n_columns_left)
      if i not in left_indices]
    nonjoin_column_names += [right_table.column_names[i] for i in range(n_columns_right)
      if i not in right_indices]

    join_result_column_names = join_column_names + nonjoin_column_names

    return join_result_column_names

  def _qualify_join_conditions(join_conditions, table_list):
    
    qualified_conds = []

    for cond in join_conditions:
      qualified_lhs = _qualify_column_names([cond[0]], table_list)
      qualified_rhs = _qualify_column_names([cond[2]], table_list)


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
