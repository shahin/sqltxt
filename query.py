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

  @staticmethod
  def _replace_column_wildcards(column_list, replacement_columns):
    """Given a list of Columns, replace any Column named '*' with all Columns in the replacement 
    list."""
    columns_resolved_wildcards = []
    for col in column_list:
      if col.name == '*':
        columns_resolved_wildcards.extend(replacement_columns)
      else:
        columns_resolved_wildcards.append(col)

    return columns_resolved_wildcards

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

      # this is cryptic. self.join? right_subquery.from_clauses? a more functional approach
      # would be easier to test
      result_table = self.join(right_subquery.from_clauses[0][3:])

      self.missing_select_columns = []
      if right_subquery.missing_select_columns:
        # we are still missing any missing columns we don't find in the left table
        self.missing_select_columns = [
          col for col in right_subquery.missing_select_columns
          if not col.match(self.left_table.columns)
          ]

    else:

      self.missing_select_columns = [col for col in self.columns
        if not col.match(self.left_table.columns)]

      result_table = self.left_table

    self.columns = self._replace_column_wildcards(self.columns, result_table.columns)

    where_conditions = self._normalize_sql_boolean_operators(self.where_clauses)
    result_table.subset_columns(where_conditions)
    
    # order result columns to match the select list via a Table method
    result_table.order_columns(
        [col for col in self.columns if col not in self.missing_select_columns], 
        drop_other_columns = self.is_top_level)

    return result_table
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
