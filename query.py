from sql_tokenizer import SqlTokenizer
from table import Table
from copy import deepcopy

class Query:

  def __init__(self, column_names, from_clauses, where_clauses):

    self.from_clauses = from_clauses
    self.column_names = column_names
    # self.qualified_names = self._qualify_column_names()
    self.tables = {}
    self.where_clauses = where_clauses

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
      del left_subquery.from_clauses[-1]
      right_subquery = deepcopy(self)
      del right_subquery.from_clauses[:-1]
      joined_table = self.join(left_subquery.generate_table(),right_subquery.generate_table())
      return joined_table

    elif n_from_clauses == 1:
      if len(self.from_clauses[0]) == 1:
        table_name = self.from_clauses[0][0]
      else:
        table_name = self.from_clauses[0][1]
      table = Table(table_name)

      #import pdb; pdb.set_trace()

      if self.where_clauses is not None:
        where_conditions = self._normalize_sql_boolean_operators(self.where_clauses)
        table.select_subset(where_conditions)
      
      #if self.order_by is not None:
      #  table.sort()
      table.order_columns(self.column_names)
      return table
    
    return None


  def _qualify_column_names(self):
    """Return a list of lists of qualified column names for the select columns."""

    qualified_names = {}
    
    for col_name in self.column_names:
      names_for_this_col = []
      for table_name in self.from_clauses.keys():
        if col_name in self.from_clauses[table_name].column_names:
          names_for_this_col.append(table_name + '.' + col_name)
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
