from column import Column
from table import Table

import logging
LOG = logging.getLogger(__name__)

def join(left_table, right_table, join_type, join_conditions):
    """Return a Table representing the join of the left and right Tables of this Query."""

    LOG.debug('Performing join on ({0})'.format(
      ', '.join([' '.join(c) for c in join_conditions])))

    # find the indices of the columns used in the join conditions
    left_indices, right_indices = _get_join_indices(left_table, right_table, join_conditions)

    # re-sort tables if necessary
    if not left_table.is_sorted_by(left_indices):
        LOG.debug('Table {0} not sorted prior to join'.format(left_table))
        left_table.sort([left_table.columns[i] for i in left_indices])

    if not right_table.is_sorted_by(right_indices):
        LOG.debug('Table {0} not sorted prior to join'.format(right_table))
        right_table.sort([right_table.columns[i] for i in right_indices])

    # constract the command that will join the data
    left_indices_arg = ','.join([str(li + 1) for li in left_indices])
    right_indices_arg = ','.join([str(ri + 1) for ri in right_indices])

    join_cmd = "join -t, -1 {0} -2 {1} <({2}) <({3})".format(
        left_indices_arg, right_indices_arg, 
        left_table.get_cmd_str(), right_table.get_cmd_str())

    join_columns = _join_columns(left_table, right_table, left_indices, right_indices)

    # create a new Table representing the (non-materialized) result of the join command
    join_result_table = Table.from_cmd(
        name = 'join_result',
        cmd = join_cmd,
        columns = join_columns
    )

    return join_result_table

def _get_join_indices(left_table, right_table, join_conditions):
    """Given the join conditions, return the indices of the columns used in the join."""

    # only equality joins supported here
    # only 'and' joins supported here
    left_indices = []
    right_indices = []
    for condition in join_conditions:
        LOG.debug('Join condition {0}'.format(condition))
     
    join_vars = (condition['left_operand'], condition['right_operand'])

    for join_var in join_vars:

        join_col = Column(join_var)
        assert not (join_col in left_table.columns and join_col in right_table.columns)

        if join_col in left_table.columns:
            left_indices.append(left_table.column_idxs[join_col][0])
        elif join_col in right_table.columns:
            LOG.debug('Right column idxs: {0}'.format(right_table.column_idxs))
            right_indices.append(right_table.column_idxs[join_col][0])

    assert left_indices != []
    assert right_indices != []
    return left_indices, right_indices

def _join_columns(left_table, right_table, left_indices, right_indices):
    """Given the indices of join columns, return the ordered column names in the joined result."""

    n_columns_left = len(left_table.columns)
    n_columns_right = len(right_table.columns)

    join_columns = [left_table.columns[i] for i in left_indices]

    nonjoin_columns = [left_table.columns[i] for i in range(n_columns_left) 
        if i not in left_indices]
    nonjoin_columns += [right_table.columns[i] for i in range(n_columns_right)
        if i not in right_indices]

    join_result_columns = join_columns + nonjoin_columns
    LOG.debug('Resolved join result column names as [{0}]'.format(
        ', '.join([repr(c) for c in join_result_columns])))

    return join_result_columns
