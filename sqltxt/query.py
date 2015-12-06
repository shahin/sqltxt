from column import Column
from table import Table
from joins import join_tables
from ordered_set import OrderedSet

import logging
LOG = logging.getLogger(__name__)

class Query(object):
    """Create Tables and perform operations on them."""

    def __init__(self, from_clause, where_clauses, columns = None, is_top_level = True):
        """Instantiate a new Query from tokenized SQL clauses."""

        self.is_top_level = is_top_level

        self.from_clause = from_clause
        self.where_clauses = where_clauses
        self.columns = [Column(c) if not isinstance(c, Column) else c for c in columns]

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

    @staticmethod
    def split_from_clause(from_clause):
        """Given a list of join clauses, return a new from-clause with a leftmost relation and
        the rest of the join clauses as a new join clause."""

        joins = from_clause['joins'][0]

        left_from_clause = { 'left_relation': from_clause['left_relation'] }
        right_from_clause = {
            'left_relation': {
                'path': joins[0]['right_relation']['path'],
                'alias': joins[0]['right_relation']['alias']
            },
        }

        remaining_joins = joins[1:]
        if remaining_joins:
            right_from_clause['joins'] = [remaining_joins]
        
        join_type = joins[0]['join_type'] or 'inner'
        join_conditions = joins[0]['join_conditions']
        
        return left_from_clause, right_from_clause, join_type, join_conditions

    def generate_table(self):
        """Return a Table representing the result of this Query.

        For Queries with no joins, this method uses Table methods to perform the standard subsetting
        and ordering operations.

        For Queries with joins across n Tables, this method 
          1. instantiates a new Query representing the subquery across the right-most n-1 Tables,
          2. calls generate_table on the sub-Query, and
          3. returns the result of the 2-way join between the left-most Table of this query and the 
          result of the sub-Query (which is also a Table).
        """

        if 'joins' in self.from_clause:

            left_from_clause, right_from_clause, join_type, join_conditions = \
                self.split_from_clause(self.from_clause)

            subquery_columns = self.get_subquery_columns(join_conditions)

            right_subquery = Query(right_from_clause, [], subquery_columns, is_top_level=False)
            left_subquery = Query(left_from_clause, [], subquery_columns, is_top_level=False)
            self.right_table = right_subquery.generate_table()
            self.left_table = left_subquery.generate_table()

            self.result_table = join_tables(self.left_table, self.right_table, join_type, join_conditions)

        else:
            table_path = self.from_clause['left_relation']['path']
            table_alias = self.from_clause['left_relation']['alias'][0] or table_path
            self.result_table = Table.from_file_path(table_path, alias=table_alias)

        where_conditions = _normalize_sql_boolean_operators(self.where_clauses)
        self.result_table.subset_rows(where_conditions)
        
        # order result columns to match the select list via a Table method
        self.order_output_columns()
        return self.result_table

    def get_subquery_columns(self, join_conditions):
        subquery_columns = OrderedSet(self.columns)
        for jc in join_conditions:
            for col_name in (jc['left_operand'], jc['right_operand']):
                subquery_columns.add(Column(col_name))
        return subquery_columns

    def order_output_columns(self):

        if self.columns == self.result_table.columns:
            return None

        output_columns = []
        for query_col in self.columns:
            for result_col in self.result_table.columns:
                if query_col == result_col:
                    output_columns.append(result_col)

        self.result_table.order_columns(
            output_columns,
            drop_other_columns = self.is_top_level)
  
def _normalize_sql_boolean_operators(sql_where_clauses):
    """Given tokenized SQL where clauses, return their translations to normal boolean operators."""

    comparison_operators = {
        '=': '==',
        'eq': '==',
        'ne': '!=',
        'ge': '>=',
        'gt': '>',
        'le': '<=',
        'lt': '<'
        }
    logical_operators = {
        'and': ' && ',
        'or': ' || ',
        }

    bool_where_clauses = []

    # translate SQL-specific boolean operators to the tokens that normal languages use
    if len(sql_where_clauses) > 0:
        for clause in sql_where_clauses:
            if clause in logical_operators:
                bool_where_clauses.append(logical_operators[clause])
            else:
                bool_clause = [ comparison_operators.get(token, token) for token in clause ]
                bool_where_clauses.append(bool_clause)

    return bool_where_clauses
