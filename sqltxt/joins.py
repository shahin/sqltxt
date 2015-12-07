from column import Column
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

    _name_composite_columns(join_result_table, parsed_join_conditions)

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
            'left_operand': Column(cond['left_operand']),
            'right_operand': Column(cond['right_operand'])
        })

    return parsed_conditions

def _get_join_indices(left_table, right_table, join_conditions):
    """Given the join conditions, return the indices of the columns used in the join."""

    left_indices = []
    right_indices = []
    for cond in join_conditions:
        for join_col in (cond['left_operand'], cond['right_operand']):

            assert not (join_col in left_table.columns and join_col in right_table.columns)

            if join_col in left_table.columns:
                left_indices.append(left_table.column_idxs[join_col][0])
            elif join_col in right_table.columns:
                right_indices.append(right_table.column_idxs[join_col][0])
            else:
                raise ValueError('Column {0} not found on tables {1} or {2}'.format(
                    join_var,
                    left_table,
                    right_table
                    ))

    return left_indices, right_indices

def _join_columns(left_table, right_table, indices):
    """Given the indices of join columns, return the ordered column names in the joined result."""

    n_columns_left = len(left_table.columns)
    n_columns_right = len(right_table.columns)

    join_columns = []
    for li, ri in indices:
        join_columns.append(li)
        join_columns[-1].names += ri.names

    nonjoin_columns = [left_table.columns[i] for i in range(n_columns_left) 
        if i not in left_indices]
    nonjoin_columns += [right_table.columns[i] for i in range(n_columns_right)
        if i not in right_indices]

    join_result_columns = join_columns + nonjoin_columns
    LOG.debug('Resolved join result column names as [{0}]'.format(
        ', '.join([repr(c) for c in join_result_columns])))

    return join_result_columns

def _name_composite_columnds(table, join_conditions):
