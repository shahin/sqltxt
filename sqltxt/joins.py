from column import Column, ColumnName, merge_columns
from table import Table

import logging
LOG = logging.getLogger(__name__)

def join_tables(left_table, right_table, join_type, join_conditions):
    """Return a Table representing the join of the left and right Tables of this Query."""

    LOG.debug('Performing join on ({0})'.format(
      ', '.join([' '.join(c) for c in join_conditions])))

    # find the indices of the columns used in the join conditions
    parsed_join_conditions = parse_join_conditions(join_conditions)
    indices = _get_join_indices(left_table, right_table, parsed_join_conditions)
    left_indices = [li for li, ri in indices]
    right_indices = [ri for li, ri in indices]

    # re-sort tables if necessary
    if not left_table.is_sorted_by(left_indices):
        LOG.debug('Table {0} not sorted prior to join'.format(left_table))
        left_table.sort([left_table.columns[i] for i in left_indices])

    if not right_table.is_sorted_by(right_indices):
        LOG.debug('Table {0} not sorted prior to join'.format(right_table))
        right_table.sort([right_table.columns[i] for i in right_indices])

    # construct the command that will join the data
    left_indices_arg = ','.join([str(li + 1) for li in left_indices])
    right_indices_arg = ','.join([str(ri + 1) for ri in right_indices])

    join_cmd = "join -t, -1 {0} -2 {1} <({2}) <({3})".format(
        left_indices_arg, right_indices_arg, 
        left_table.get_cmd_str(), right_table.get_cmd_str())

    join_columns = _join_columns(left_table, right_table, indices)

    # create a new Table representing the (non-materialized) result of the join command
    join_result_table = Table.from_cmd(
        name = 'join_result',
        cmd = join_cmd,
        columns = join_columns
    )

    return join_result_table

def parse_join_conditions(join_conditions):
    """Given join conditions defined as string tokens in a dictionary, return a validated set of join
    conditions defined as Column objects in a dictionary."""

    for condition in join_conditions:
        if condition['operator'] != '=':
            raise ValueError('Operator {} not supported; only equality joins are supported.'.format(
                operator))
    
    n_cond = len(join_conditions)
    if n_cond > 1:
        raise ValueError('Only 1 join condition is supported, but {} were found.'.format(n_cond))
     
    parsed_conditions = []
    for cond in join_conditions:
        parsed_conditions.append({
            'operator': cond['operator'],
            'left_operand': ColumnName(cond['left_operand']),
            'right_operand': ColumnName(cond['right_operand'])
        })

    return parsed_conditions

def _get_join_indices(left_table, right_table, join_conditions):
    """Given the join conditions, return the indices of the columns used in the join."""

    left_indices = []
    right_indices = []
    for cond in join_conditions:
        for join_col_name in (cond['left_operand'], cond['right_operand']):

            left_col = left_table.get_column_for_name(join_col_name)
            right_col = right_table.get_column_for_name(join_col_name)

            assert not (left_col and right_col)

            if left_col:
                left_indices.append(left_table.column_idxs[left_col][0])
            elif right_col:
                right_indices.append(right_table.column_idxs[right_col][0])
            else:
                raise ValueError('Column name {0} not found on tables {1} or {2}'.format(
                    join_col_name,
                    left_table,
                    right_table
                    ))

    return zip(left_indices, right_indices)

def _join_columns(left_table, right_table, join_indices):
    """Given the indices of join columns, return the ordered column names in the joined result."""

    join_columns = _resolve_join_columns(left_table, right_table, join_indices)
    nonjoin_columns = _resolve_nonjoin_columns(left_table, right_table, join_indices)

    join_result_columns = join_columns + nonjoin_columns
    LOG.debug('Resolved join result column names as [{0}]'.format(
        ', '.join([repr(c) for c in join_result_columns])))

    return join_result_columns

def _resolve_nonjoin_columns(left_table, right_table, indices):
    left_indices, right_indices = zip(*indices)
    nonjoin_columns = [left_table.columns[i] for i in range(len(left_table.columns)) 
        if i not in left_indices]
    nonjoin_columns += [right_table.columns[i] for i in range(len(right_table.columns))
        if i not in right_indices]
    return nonjoin_columns

def _resolve_join_columns(left_table, right_table, indices):
    """Return the post-join list of join columns in the result table.

    In SQL, we can SELECT by name either column of a pair of columns that have been joined. But
    coreutils' join tool writes only one column for each input pair that have been joined on. So this
    output column needs to have the names of both input columns to conform to SQL SELECT rules.
    """
    return [ merge_columns(left_table.columns[li], right_table.columns[ri]) for li, ri in indices ]
