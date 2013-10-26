from sql_tokenizer import SqlTokenizer
from column import Column
from table import Table
import logging

class Query:
  """Create Tables and perform operations on them."""

  LOG = logging.getLogger(__name__)

  def __init__(self, from_clauses, where_clauses, column_names = None, is_top_level = True):
    """Instantiate a new Query from tokenized SQL clauses."""

    self.is_top_level = is_top_level

    self.from_clauses = from_clauses
    self.where_clauses = where_clauses
    self.column_names = column_names
    self.columns = [Column(column_name) for column_name in self.column_names]

    self.missing_select_columns = None

    self.left_table = None
    self.right_table = None

  def generate_table(self):
    """Return a Table representing the result of this Query.

    For Querys with no joins, this method uses Table methods to perform the standard subsetting
    and ordering operations.

    For Querys with joins across n Tables, this method 
      1. instantiates a new sub-Query representing the query across the right-most n-1 Tables,
      2. calls generate_table on the sub-Query, and
      3. returns the result of the 2-way join between the left-most Table of this query and the 
      result of the sub-Query (which is also a Table).
    """

    # instantiate the left-most Table in all the where clauses
    first_from_clause_tokens = self.from_clauses[0]
    if len(first_from_clause_tokens) > 1:
      # first where clause is a join clause
      self.left_table = Table.from_filename(first_from_clause_tokens[1].upper())
    else:
      # first where clause is not a join clause
      self.left_table = Table.from_filename(first_from_clause_tokens[0].upper())

    if len(self.from_clauses) > 1:
      # instantiate the right Table as the result of a Query on all tables other than
      # the left-most

      right_subquery = Query(self.from_clauses[1:], [], self.column_names, is_top_level = False)
      self.right_table = right_subquery.generate_table()

      self.missing_select_columns = []
      if right_subquery.missing_select_columns:
        # we are still missing any missing columns we don't find in the left table
        self.missing_select_columns = [
          col for col in right_subquery.missing_select_columns
          if not col.search(self.left_table.columns)
          ]

      joined_table = self.join(right_subquery.from_clauses[0][3:])

      # replace wildcards in the select list with table column names
      columns_resolved_wildcards = []
      for col in self.columns:
        if col.name == '*':
          columns_resolved_wildcards.extend(joined_table.columns)
        else:
          columns_resolved_wildcards.append(col)

      self.columns = columns_resolved_wildcards

      if self.where_clauses:
        where_conditions = self._normalize_sql_boolean_operators(self.where_clauses)
        joined_table.select_subset(where_conditions)
      
      # order result columns to match the select list via a Table method
      joined_table.order_columns(
          [col for col in self.columns if col not in self.missing_select_columns], 
          drop_other_columns = self.is_top_level)

      return joined_table

    else:

      # replace wildcards in the select list with table column names
      columns_resolved_wildcards = []
      for col in self.columns:
        if col.name == '*':
          columns_resolved_wildcards.extend(self.left_table.columns)
        else:
          columns_resolved_wildcards.append(col)

      self.columns = columns_resolved_wildcards

      # identify column names in the Query's select list that do not exist in its tables
      self.missing_select_columns = [col for col in self.columns
        if not col.search(self.left_table.columns)]

      # apply where conditions via a Table method
      if self.where_clauses:
        where_conditions = self._normalize_sql_boolean_operators(self.where_clauses)
        self.left_table.select_subset(where_conditions)

      # order result columns to match the select list via a Table method
      self.left_table.order_columns(
          [col for col in self.columns if col not in self.missing_select_columns], 
          drop_other_columns = self.is_top_level)

      return self.left_table

  def join(self, join_conditions):
    """Return a Table representing the join of the left and right Tables of this Query."""

    self.LOG.debug('Performing join on ({0})'.format(
      ', '.join([' '.join(c) for c in join_conditions])))

    # find the indices of the columns used in the join conditions
    left_indices, right_indices = self._get_join_indices(join_conditions)

    # re-sort tables if necessary
    if not self.left_table.is_sorted_by(left_indices):
      self.left_table.sort([self.left_table.columns[i] for i in left_indices])

    if not self.right_table.is_sorted_by(right_indices):
      self.right_table.sort([self.right_table.columns[i] for i in right_indices])

    # constract the command that will join the data
    left_indices_arg = ','.join([str(li + 1) for li in left_indices])
    right_indices_arg = ','.join([str(ri + 1) for ri in right_indices])

    join_cmd = "join -t, -1 {0} -2 {1} <({2}) <({3})".format(
      left_indices_arg, right_indices_arg, 
      self.left_table.get_cmd_str(), self.right_table.get_cmd_str())

    join_column_names = self._join_column_names(left_indices, right_indices)

    # create a new Table representing the (non-materialized) result of the join command
    join_result_table = Table.from_cmd(
      name = 'join_result',
      cmd = join_cmd,
      columns = join_column_names
      )

    return join_result_table

  def _get_join_indices(self, join_conditions):
    """Given the join conditions, return the indices of the columns used in the join."""

    # only equality joins supported here
    # only 'and' joins supported here
    left_indices = []
    right_indices = []
    for condition in join_conditions:

      join_vars = (condition[0], condition[2])

      for join_var in join_vars:

        join_col = Column(join_var)
        if join_col.table_name == self.left_table.name:
          left_indices.append(self.left_table.column_idxs[join_col])
        elif join_col.table_name == self.right_table.name:
          right_indices.append(self.right_table.column_idxs[join_col])

    return left_indices, right_indices

  def _join_column_names(self, left_indices, right_indices):
    """Given the indices of join columns, return the ordered column names in the joined result."""

    n_columns_left = len(self.left_table.columns)
    n_columns_right = len(self.right_table.columns)

    join_columns = [self.left_table.columns[i] for i in left_indices]
    nonjoin_columns = [self.left_table.columns[i] for i in range(n_columns_left) 
      if i not in left_indices]
    nonjoin_columns += [self.right_table.columns[i] for i in range(n_columns_right)
      if i not in right_indices]

    # TODO: need columns that we join on to have multiple column table qualifiers so that
    # we can apply where conditions to columns that have multiple table parents, e.g.
    # table_a join table_b on (table_a.col_a = table_b.col_a) where table_b.col_a = 2
    # 'where' is applied after the join, but by then, table_b.col_a no longer exists (although
    # table_a.col_a still exists)
    # options:
    # 1. mutliple column qualifiers as described above
    # 2. re-qualify all table columns under the joined table's name, and re-qualify any applied
    # statements using that joined table's name
    # 3. re-qualify all table columns under the joined table's name which includes ancestor
    # table info, then do name matching against qualified columns in select statements
    join_result_columns = join_columns + nonjoin_columns
    self.LOG.debug('Resolved join result column names as [{0}]'.format(
      ', '.join([repr(c) for c in join_result_columns])))

    return join_result_columns
  
  def _normalize_sql_boolean_operators(self, sql_where_clauses):
    """Given tokenized SQL where clauses, return their translations to normal boolean operators."""

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
