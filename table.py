from column import Column
import logging
import re

LOG = logging.getLogger(__name__)

def compose_cmd_str(cmds, output_target = None):
  """Given a Table's commands, return the executable string for those commands.
  
  :param cmds: a list of command strings and dictionaries of command strings keyed by outputs
  """

  cmd_strs = []

  for cmd in cmds:

    if isinstance(cmd, str):
      # just a regular command within a pipeline
      cmd_strs.append(cmd + ' | ')

    elif isinstance(cmd, list):
      # a discrete pipeline that is terminated at the end
      cmd_str = compose_cmd_str(cmd)
      if cmd_str[-1] == '&':
        # run this pipeline as a background process
        cmd_str = cmd_str[:-1].rstrip(' |') + ' & '
      else:
        cmd_str += '; '
      cmd_strs.append(cmd_str)

    else:
      # a command that should be teed into a fifo within a pipeline
      # expecting a dictionary-like set of commands keyed by output names
      targets = list(cmd.keys())
      targets.sort()
      for target in targets:
        cmd_strs.append('tee >(' + compose_cmd_str(cmd[target], target) + ')' + ' | ')

  composed_cmd_str = ''.join(cmd_strs).rstrip(' |')
  if output_target:
    composed_cmd_str += (' > ' + output_target)

  return composed_cmd_str


def join_tables(left_table, right_table, join_conditions):
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

  join_columns = _join_columns(left_table.columns, left_indices, right_table.columns, right_indices)

  # create a new Table representing the (non-materialized) result of the join command
  join_result_table = Table.from_cmd(
    name = 'join_result',
    cmd = join_cmd,
    columns = join_columns
    )

  return join_result_table

def _get_join_indices(left_table, right_table, join_conditions):
  """Given the join conditions, return the indices of the columns used in the join."""

  # only equality joins supported
  # only 'and' conditions supported
  left_indices = []
  right_indices = []
  for condition in join_conditions:
    LOG.debug('Join condition {0}'.format(condition))

    join_vars = (condition[0], condition[2])

    for join_var in join_vars:

      join_col = Column(join_var)
      if join_col.table_name.upper() == left_table.name.upper():
        left_indices.append(left_table.column_idxs[join_col][0])
      elif join_col.table_name.upper() == right_table.name.upper():
        right_indices.append(right_table.column_idxs[join_col][0])

  return left_indices, right_indices

def _join_columns(left_columns, left_join_indices, right_columns, right_join_indices):
  """Given the indices of join columns, return the ordered columns of the joined table."""

  n_columns_left = len(left_columns)
  n_columns_right = len(right_columns)

  join_columns = [Column(str(left_columns[i])) for i in left_join_indices]
  for idx, col in enumerate(join_columns):
    col.ancestors = \
      [right_columns[right_join_indices[idx]]] + \
      [left_columns[left_join_indices[idx]]]

  nonjoin_columns = [left_columns[i] for i in range(n_columns_left) 
    if i not in left_join_indices]
  nonjoin_columns += [right_columns[i] for i in range(n_columns_right)
    if i not in right_join_indices]

  join_result_columns = join_columns + nonjoin_columns
  LOG.debug('Resolved join result column names as [{0}]'.format(
    ', '.join([repr(c) for c in join_result_columns])))

  return join_result_columns


class Table:
  """Translate abstract data-manipulation operations to commands that perform them.

  A Table is a virtual representation of data. Operations on Tables are accumulated and
  optimized, but a Table cannot execute its own commands. In order retrieve the data represented
  by a Table, a second party must execute the Table's commands.
  """

  VALID_IDENTIFIER_REGEX = '^[a-zA-Z_][a-zA-Z0-9_.]*$'
  LOG = logging.getLogger(__name__)

  fifos = []

  SUPPORTED_AGGREGATE_FUNCTIONS = ['COUNT']

  # TODO: make the output command str use column aliases instead of column names
  # TODO: test that an aggregate function Column gets an empty index list in compute column idxs
  # TODO: stop passing aggregate functions down through subqueries since they don't exist on tables and won't be applied until the top-level query anyway

  def __str__(self):
    return self.name

  def __init__(self, 
    name, delimiter = ',', cmd = None, columns = None, is_file = False, extension = 'txt'):

    self.name = name
    self.delimiter = delimiter
    self.cmds = [] if cmd == None else [cmd]

    # every Table needs to qualify all incoming columns with its own name, keeping
    # ancestries intact
    self.columns = columns
    self.columns = [self._qualify_column(col) for col in self.columns]
    self.LOG.debug('{0} has columns {1}'.format(self, self.columns))

    # verify that all aggregate functions are supported by this Table
    for column in self.columns:
      if column.is_aggregate_function:
        if column.alias not in self.__class__.SUPPORTED_AGGREGATE_FUNCTIONS:
          raise ValueError("Aggregate function '{0}' is not supported.".format(column.alias))

    self.is_file = is_file
    self.extension = extension

    self.sorted_by = []
    self.outfile_name = "{0}.out".format(name)

    self.column_idxs = self._compute_column_indices()

  @property 
  def column_names(self):
    return [col.name for col in self.columns]

  @classmethod
  def from_filename(cls, file_path, columns = None, delimiter = ',', extension = 'txt'):
    """Given the path to a file, instantiate a Table representing that file.
    
    :param file_path: a string containing the path to the file
    :param columns: an exhaustive list of column names or Column objects on this table
    :param delimiter: the column delimiter for this table; defaults to ','
    :param extension: a string containing the file extension; defaults to 'txt'
    """

    if columns == None:
      columns = cls._parse_column_names(file_path + '.' + extension, delimiter)

    for idx, col in enumerate(columns):
      if not isinstance(col, Column):
        columns[idx] = Column(col)

    return cls(file_path, delimiter, None, columns, True, extension)

  @classmethod
  def from_cmd(cls, name, cmd, columns, delimiter = ','):
    """Given a command, instantiate a Table representing the output of that command.
    
    :param name: the name of the table
    :param cmd: a string of commands whose execution materializes this table
    :param columns: an exhaustive list of column names or Column objects on this table
    :param delimiter: the column delimiter for this table; defaults to ','
    """

    for idx, col in enumerate(columns):
      if not isinstance(col, Column):
        columns[idx] = Column(col)

    return cls(name, delimiter, cmd, columns)

  @staticmethod
  def _parse_column_names(file_path, delimiter):
    """Return a list of column headers found in the first line of a file."""

    with open(file_path) as table_file:
      head = table_file.readline().rstrip()

    return head.split(delimiter)

  def _qualify_column(self, col):
    """If the given Column either:
    
    1. Does not match any of this Table's Columns, or
    2. Does match exactly one of this Table's Columns and has no Table qualifier
    
    then qualify it with this Table's name."""

    matching_cols = col.match(self.columns)
    if len(matching_cols) > 0:
      if not col.table_name:
        col.table_name = self.name
      elif col.table_name != self.name:
        # TODO: occurs primarily during Table initialization, may want to break out
        parent_col = col
        col = Column('{0}.{1}'.format(self.name, str(col)), [parent_col])
      return col

    col.table_name = self.name
    return col

  def order_columns(self, columns_in_order, drop_other_columns = False):
    """Rearrange and subset the columns of this Table."""

    columns_in_order = [self._qualify_column(col) for col in columns_in_order]

    if (columns_in_order == self.columns) or (
      columns_in_order == self.columns[0:len(columns_in_order)] and not drop_other_columns):
      self.LOG.debug('Columns already in order {0}'.format(self.columns))
      return

    self.LOG.debug('Current column order of {0} is {1}'.format(self.name, self.columns))
    self.LOG.debug('Reordering {0} columns to {1}'.format(self.name, columns_in_order))
    
    reordered_col_idxs = [self.column_idxs[col][0] for col in columns_in_order]
    unchanged_col_idxs = [
      self.column_idxs[col][0] for col in self.columns
      if col not in columns_in_order]

    col_idxs = reordered_col_idxs
    if not drop_other_columns:
      col_idxs += unchanged_col_idxs

    reorder_cmd = "awk -F'{0}' 'OFS=\"{0}\" {{ print {1} }}'".format(
      self.delimiter, ','.join('$' + str(idx + 1) for idx in col_idxs))

    self.columns = [self.columns[idx] for idx in col_idxs]
    self.column_idxs = self._compute_column_indices()
    self.cmds.append(reorder_cmd)

  def is_sorted_by(self, sort_order_indices):
    """Return true if this Table's rows are sorted by columns at the given indices."""

    if len(self.sorted_by) < len(sort_order_indices):
      return False

    for target_idx, source_idx in enumerate(sort_order_indices):
      if self.columns[source_idx] != self.sorted_by[target_idx]:
        return False

    return True

  def _dedupe_with_order(self,dupes):
    """Given a list, return it without duplicates and order preserved."""

    seen = set()
    deduped = []
    for c in dupes:
      if c not in seen:
        seen.add(c) 
        deduped.append(c)
    return deduped

  def sort(self, columns_to_sort_by):
    """Sort the rows of this Table by the given columns."""

    deduped_columns = self._dedupe_with_order(columns_to_sort_by)
    columns_to_sort_by = [self._qualify_column(col) for col in deduped_columns]

    # if this table is already sorted by the requested sort order, do nothing
    if len(columns_to_sort_by) <= len(self.sorted_by):
      if columns_to_sort_by == self.sorted_by[0:len(columns_to_sort_by)]:
        return
    self.LOG.debug('Sorting {0} by {1}'.format(self.name, columns_to_sort_by))

    column_idxs_to_sort_by = [self.column_idxs[col][0] for col in columns_to_sort_by]

    sort_key_params = ' -k '.join(
          ','.join([str(idx + 1),str(idx + 1)]) for idx in column_idxs_to_sort_by)

    sort_cmd = 'sort -t{0} -k {1}'.format(self.delimiter, sort_key_params)
    self.sorted_by = columns_to_sort_by
    self.cmds.append(sort_cmd)
    
  def subset_columns(self, conditions):
    """Subset the rows of this Table to rows that satisfy the given conditions."""

    # translate a list of boolean conditions to awk syntax
    condition_str = ''
    for expr_part in conditions:
      if expr_part == 'and':
        condition_str += ' && '
      elif expr_part == 'or':
        condition_str += ' || '
      else:
        # treat any PostgreSQL-valid identifier as a column
        expr_part = [
          ('$' + str( self._column_idx(self._qualify_column(Column(token)), include_ancestors = True) ) 
            if re.match(self.VALID_IDENTIFIER_REGEX, token) 
            else token
            )
          for token in expr_part]
        condition_str += ' '.join(expr_part)

    if condition_str == '':
      self.LOG.debug('Empty condition string so not subsetting columns on {0}'.format(
          self.name))
      return

    columns = ','.join(['$' + str(self.column_idxs[c][0] + 1) for c in self.columns])
    awk_cmd = "awk -F'{0}' 'OFS=\"{0}\" {{ if ({1}) {{ print {2} }} }}'".format(
      self.delimiter, condition_str, columns)
    self.cmds.append(awk_cmd)

  def group_by(self, columns_to_group_by):
    """De-duplicate rows by the given columns to group by and drop all other columns."""

    cut_columns = ','.join([str(self.column_idxs[c][0] + 1) for c in columns_to_group_by])
    group_cmds = [
      'cut -d, -f{0}'.format(cut_columns),
      sort_cmd + ' -u'
      ]

    # join any aggregate function FIFOs by the group-by variables

  def count(self, columns_to_group_by, count_column_name = None, distinct = False):
    """Count the number of rows for each set of values found for the given columns to group by.

    :param columns_to_group_by: a list of column names or Column objects to group by
    :param count_column_name: the name of a column for which only non-null values are counted.
      If None, then count all rows. Defaults to None.
    :param distinct: if True, duplicates do not count. Defaults to False.

    Sets of group-by values with zero count will not be retained.
    """

    cmds = []

    if columns_to_group_by:

      column_idxs_to_group_by = [self.column_idxs[c][0] + 1 for c in columns_to_group_by]
      cut_columns = ','.join([str(c) for c in column_idxs_to_group_by])
      sort_key_params = ' -k '.join([str(idx) + ',' + str(idx) for idx in column_idxs_to_group_by])
      sort_cmd = 'sort -t{0} -k {1}'.format(self.delimiter, sort_key_params)

      if distinct:
        sort_cmd = sort_cmd + ' -u'

      fifo_name = 'count_group_by_' + str(len(self.fifos))
      self.fifos.append(fifo_name)

      cmds = [
        'cut -d, -f{0}'.format(cut_columns),
        sort_cmd,
        'uniq -c',
        "awk -F' ' '{ print $2, $1 }'"
      ]

    else:

      cmds = ['wc -l']
      if distinct:
        cmds = ['sort -u'] + cmds

    if count_column_name:
      # filter out null values for the column passed to the count function
      count_column_idx = self.column_idxs[count_column_name][0] + 1
      cmds = ["awk -F'{0}' '{{ if(${1} != \"\") print }}".format(self.delimiter, count_column_idx)] + cmds

    # TODO: instead of inserting the aggregate function tee commands explicitly, create a new table and tee into it
    # this way we can use the output_target Table attribute for both the aggfunc tees and the query result
    # 1. create the fifo
    # 2. create the aggfunc table
    # 3. tee into the aggfunc table
    self.cmds = self.cmds + cmds

  def get_cmd_str(self, output_column_names = False):
    """Return a string of commands whose output is the contents of this Table.""" 

    cmds = self.cmds

    # add input from a file
    if self.is_file:
      cmds = ['tail +2 {0}.{1}'.format(self.name, self.extension)] + cmds 

    cmd_str = compose_cmd_str(cmds)

    # write column names
    if output_column_names:
      cmd_str = 'echo "{0}"; '.format(
          ','.join([str(col) for col in self.columns])
          ) + cmd_str

    return cmd_str

  def _compute_column_indices(self):
    """Return a dictionary of column index lists keyed by Column."""

    idxs = {}
    for (i,c) in enumerate(self.columns):
      try:
        idxs[c].append(i)
      except KeyError:
        idxs[c] = [i]
    self.LOG.debug('{0} computed column indices {1}'.format(self,idxs))
    return idxs
  
  def _column_idx(self, column, include_ancestors = False):
    """Given a Column, return the index of the matching column on this Table. 
    
    If a matching column does not exist, optionally match the ancestors of columns on this 
    Table until a match is found. 
    
    Raises a KeyError if no matches are found.
    """

    try:
      return self.column_idxs[column][0] + 1
    except KeyError as e:
      if include_ancestors:
        # if this column doesn't match anything on this table, try this column's ancestors
        # traverse ancestor tree depth-first for a match
        matches = column.match(self.columns, include_ancestors)

      # all attempts to find a matching column are exhausted
      raise e
