from column import Column
import logging
import re

class Table:
  """Translate abstract data-manipulation operations to commands that perform them.

  A Table is a virtual representation of data. Operations on Tables are accumulated and
  optimized, but a Table cannot execute its own commands. In order retrieve the data represented
  by a Table, a second party must execute the Table's commands.
  """

  VALID_IDENTIFIER_REGEX = '^[a-zA-Z_][a-zA-Z0-9_.]*$'
  LOG = logging.getLogger(__name__)

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
    group_values = Table.from_cmd('group_vals', count_cmds, columns_to_group_by, self.delimiter)
    

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

      cmds = cmds + [
        'cut -d, -f{0}'.format(cut_columns),
        sort_cmd,
        'uniq -c',
        "awk -F' ' '{ print $2, $1 }'"
      ]

    else:

      cmds = cmds + ['wc -l']
      if distinct:
        sort_cmd = ['sort -u']
        cmds = sort_cmd + cmds

    if count_column_name:
      count_column_idx = self.column_idxs[count_column_name][0] + 1
      cmds = ["awk -F'{0}' '{{ if(${1} != \"\") print }}".format(self.delimiter, count_column_idx)] + cmds

    self.cmds = self.cmds + cmds

  def tee(self, cmds):
    """Tees the output of this Table's commands to the given list of cmds."""


  def get_cmd_str(self, output_column_names = False):
    """Return a string of commands whose output is the contents of this Table.""" 

    cmds = self.cmds

    # add input from a file
    if self.is_file:
      cmds = ['tail +2 {0}.{1}'.format(self.name, self.extension)] + cmds 

    cmd_str = ' | '.join(cmds)

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
