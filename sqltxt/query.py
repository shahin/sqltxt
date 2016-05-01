import itertools

from ordered_set import OrderedSet

from column import ColumnName, AmbiguousColumnNameError, UnknownColumnNameError
from table import Table
from joins import join_tables
from plan import plan
from expression import get_cnf_conditions

import logging
LOG = logging.getLogger(__name__)

def stage_columns(tables, column_names):
    """Given a list of tables and a list of ColumnNames, return a list of lists of ColumnNames with
    each ColumnName at the index of the first table (from left to right) that it's available on.

    Raises AmbiguousColumnNameError if a ColumnName is available on more than one table. Raises
    UnknownColumnNameError if the column name can't be found on any of the given tables.
    """

    staged_columns = [ [] for i in range(len(tables)) ]

    for column_name in column_names:
        matched_columns = []

        for idx, table in enumerate(tables):
            matched_column = table.get_column_for_name(column_name)
            if matched_column:
                staged_columns[idx].append(column_name)
                matched_columns.append(matched_column)

        if len(matched_columns) > 1:
            raise AmbiguousColumnNameError(column_name, matched_columns)
        elif len(matched_columns) == 0:
            raise UnknownColumnNameError(column_name)

    return staged_columns

def stage_conditions(tables, conditions):
    """Given a list of tables and a list of conditions, return a list of conditions with each at
    the index of the first table (from left to right) that it can be applied to.
    """
    condition_order = [ [] for i in range(len(tables)) ]
    for condition in conditions:
        for idx in range(len(tables)):
            if condition_applies(condition, *tables[:idx+1]):
                condition_order[idx].append(condition)
                break
    return condition_order

def condition_applies(condition, *tables):
    """Return true if all columns in the condition are found on the given tables."""

    for column_name in condition.column_names:
        matched_columns = [
            t.get_column_for_name(column_name) for t in tables
            if t.get_column_for_name(column_name) is not None
        ]

        if len(matched_columns) > 1:
            raise AmbiguousColumnNameError(column_name, matched_columns)
        elif len(matched_columns) == 0:
            return False

    return True

def classify_conditions(conditions):
    """Given a list of conditions, return equivalent conditions partitioned into those that can be
    used as join conditions and the rest as where conditions.
    """
    join_conditions = []
    where_conditions = []

    cnf_conditions = get_cnf_conditions(stringify_conditions(conditions))
    for condition in cnf_conditions:
        if condition.can_join:
            join_conditions.append(condition)
        else:
            where_conditions.append(condition)

    return join_conditions, where_conditions

def stringify_conditions(conditions):
    """Given a list of condition dictionaries with 'left_operand', 'operand', and 'right_operand'
    keys, return a list of strings expressing equivalent conditions in valid Python."""

    stringified = []
    for cond in conditions:
        if isinstance(cond, basestring):
            stringified.append(cond)
        else:
            try:
                operator = '==' if cond['operator'] == '=' else cond['operator']
                stringified.append(' '.join([str(cond['left_operand']), operator, str(cond['right_operand'])]))
            except TypeError:
                stringified.extend(['(' + stringify_conditions(cond) + ')'])

    return ' '.join(stringified)

class Query(object):
    """Create Tables and perform operations on them."""

    def __init__(self, relations, conditions=None, columns=None,
            sample_size=None, random_seed=None, is_top_level=True):

        self.relations = relations
        self.column_names = OrderedSet([
            ColumnName(c, allow_wildcard=True) if not isinstance(c, ColumnName) else c
            for c in columns
        ])

        if conditions is None:
            conditions = []
        self.join_conditions, self.where_conditions = classify_conditions(conditions)
        self.conditions = self.join_conditions + self.where_conditions

        self.sample_size = sample_size
        self.random_seed = random_seed
        self.is_top_level = is_top_level  # not a subquery

    @staticmethod
    def replace_wildcard_column_names(column_name_list, table_list):
        """Given a list of ColumnNames, replace any ColumnName that is a wildcard with all
        ColumnNames on tables with matching qualifiers (or all tables if the wildcard is
        unqualified)."""

        all_table_columns = [
            col for col in itertools.chain(*[table.columns for table in table_list])
        ]

        resolved_column_names = []
        for idx, col_name in enumerate(column_name_list):
            if col_name.is_wildcard:
                resolved_column_names.extend(OrderedSet([
                    col.alias for col in all_table_columns if col_name.match(*col.names)
                ]))
            else:
                resolved_column_names.append(col_name)

        return resolved_column_names

    def execute(self):

        self.tables = []

        for relation in self.relations:
            table_path = relation['path']
            table_alias = relation['alias']
            table = Table.from_file_path(table_path, alias=table_alias)
            self.tables.append(table)

        self.column_names = self.replace_wildcard_column_names(self.column_names, self.tables)

        # determine which columns need to be on each table on output and on input
        unstaged_condition_columns = itertools.chain(
            *[condition.column_names for condition in self.conditions]
        )
        condition_columns = stage_columns(self.tables, unstaged_condition_columns)

        # optimize join order
        join_order = plan(self.tables, self.join_conditions, self.where_conditions)
        self.tables = [self.tables[idx] for idx in join_order]

        # determine where in the join tree to apply conditions
        where_condition_stages = stage_conditions(self.tables, self.where_conditions)
        join_condition_stages = stage_conditions(self.tables, self.join_conditions)

        # use only one join condition per stage
        for stage, conditions in enumerate(join_condition_stages):
            if len(conditions) > 0:
                where_condition_stages[stage].extend(conditions[1:])
                join_condition_stages[stage] = [conditions[0]]

        # apply single-table where conditions to source tables
        multi_table_conditions =[ [] for i in range(len(self.where_conditions)) ]
        for table, conditions in zip(self.tables, where_condition_stages):
            single_table_conditions = [c for c in conditions if condition_applies(c, table)]
            table.subset_rows(single_table_conditions)
            multi_table_conditions.append(list(set(conditions) - set(single_table_conditions)))

        # build the join tree in which nodes are intermediate Tables resulting from joins
        if len(self.tables) > 1:
            result = self.execute_join(self.tables, join_condition_stages, multi_table_conditions)
        else:
            result = self.tables[0]

        result.order_columns(self.column_names, True)

        if self.sample_size is not None:
            result.sample_rows(self.sample_size, self.random_seed)

        return result

    @classmethod
    def execute_join(cls, tables, join_conditions, where_conditions):

        if len(tables) == 2:
            joined_table = join_tables(
                tables[0],
                tables[1],
                'inner',
                join_conditions[-1],
            )

        elif len(tables) > 2:
            
            left_table = cls.execute_join(
                tables[:-1],
                join_conditions[:-1],
                where_conditions[:-1],
            )

            joined_table = join_tables(
                left_table,
                tables[-1],
                'inner',
                join_conditions[-1]
            )

        else:
            raise Exception('Need at least two tables to join but only got {}'.format(tables))

        joined_table.subset_rows(where_conditions[-1])
        return joined_table

