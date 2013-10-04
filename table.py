class Table:
  """Translate abstract data-manipulation operations to commands that perform them.

  A Table is a virtual representation of data. Operations on Tables are accumulated and
  optimized, but a Table cannot execute its own commands. In order retrieve the data represented
  by a Table, a second party must execute the Table's commands.
  """

  def __init__(self, 
    name, delimiter = ',', cmd = None, column_names = None, is_file = False, extension = 'txt'):

    self.name = name
    self.delimiter = delimiter
    self.cmds = [] if cmd == None else [cmd]
    self.column_names = column_names
    self.is_file = is_file
    self.extension = extension

    self.sorted_by = []
    self.outfile_name = "{0}.out".format(name)

    self.column_idxs = self._compute_column_indices()

  @classmethod
  def from_filename(cls, file_path, column_names = None, delimiter = ',', extension = 'txt'):
    """Given the path to a file, instantiate a Table representing that file."""

    if column_names == None:
      column_names = cls._parse_column_names(file_path + '.' + extension, delimiter)

    return cls(file_path, delimiter, None, column_names, True, extension)

  @classmethod
  def from_cmd(cls, name, cmd, column_names, delimiter = ','):
    """Given a command, instantiate a Table representing the output of that command."""

    return cls(name, delimiter, cmd, column_names)

  @staticmethod
  def _parse_column_names(file_path, delimiter):
    """Return a list of column headers found in the first line of a file."""

    with open(file_path) as table_file:
      head = table_file.readline().rstrip()

    return head.split(delimiter)

  def order_columns(self, col_names_in_order, drop_other_columns = False):
    """Rearrange the columns of this Table."""
    
    reordered_col_idxs = [self.column_idxs[col_name] for col_name in col_names_in_order]
    unchanged_col_idxs = [
      self.column_idxs[col_name] for col_name in self.column_names
      if col_name not in col_names_in_order]

    col_idxs = reordered_col_idxs
    if not drop_other_columns:
      col_idxs += unchanged_col_idxs

    reorder_cmd = "awk -F'{0}' 'OFS=\"{0}\" {{ print {1} }}'".format(
      self.delimiter, ','.join('$' + str(idx + 1) for idx in col_idxs))

    self.column_names = [self.column_names[idx] for idx in col_idxs]
    self.column_idxs = self._compute_column_indices()
    self.cmds.append(reorder_cmd)

  def is_sorted_by(self, sort_order_indices):
    """Return true if this Table's rows are sorted by columns at the given indices."""

    if len(self.sorted_by) < len(sort_order_indices):
      return False

    for target_idx, source_idx in enumerate(sort_order_indices):
      if self.column_names[source_idx] != self.sorted_by[target_idx]:
        return False

    return True

  def sort(self, col_names_to_sort_by):
    """Sort the rows of this Table by the given columns."""

    # if this table is already sorted by the requested sort order, do nothing
    if len(col_names_to_sort_by) <= len(self.sorted_by):
      if col_names_to_sort_by == self.sorted_by[0:len(col_names_to_sort_by)]:
        return

    column_idxs_to_sort_by = [self.column_idxs[col_name] for col_name in col_names_to_sort_by]

    # sort-by columns must be adjacent, so reorder them if they are not adjacent
    first_idx = column_idxs_to_sort_by[0]
    last_idx = column_idxs_to_sort_by[-1]
    if column_idxs_to_sort_by != list(range(first_idx, last_idx+1)):
      self.order_columns(col_names_to_sort_by)
      column_idxs_to_sort_by = [self.column_idxs[col_name] for col_name in col_names_to_sort_by]

    sort_cmd = 'sort -t{0} -k {1}'.format(self.delimiter, 
        ','.join(str(idx + 1) for idx in column_idxs_to_sort_by))
    self.sorted_by = col_names_to_sort_by
    self.cmds.append(sort_cmd)
    

  def select_subset(self, conditions):
    """Subset the rows of this Table to rows that satisfy the given conditions."""

    # translate a list of boolean conditions to awk syntax
    condition_str = ''
    for expr_part in conditions:
      if expr_part == 'and':
        condition_str += ' && '
      elif expr_part == 'or':
        condition_str += ' || '
      else:
        expr_part = [
          ('$' + str(self.column_idxs[token] + 1) if token in self.column_idxs else token)
          for token in expr_part]
        condition_str += ' '.join(expr_part)

    # treat no-conditions as an always-true condition
    if condition_str == '':
      condition_str = '1'

    columns = ','.join(['$' + str(self.column_idxs[c] + 1) for c in self.column_names])
    awk_cmd = "awk -F'{0}' 'OFS=\"{0}\" {{ if ({1}) {{ print {2} }} }}'".format(
      self.delimiter, condition_str, columns)
    self.cmds.append(awk_cmd)

  def get_cmd_str(self, outfile_name = None, output_column_names = False):
    """Return a string of commands whose output is the contents of this Table.""" 

    cmds = self.cmds

    # add input from a file
    if self.is_file:
      cmds = ['tail +2 {0}.{1}'.format(self.name, self.extension)] + cmds 

    cmd_str = ' | '.join(cmds)

    # write column names
    if output_column_names:
      cmd_str = 'echo "{0}"; '.format(','.join(self.column_names)) + cmd_str

    # add output redirection to file
    if outfile_name is not None:
      cmd_str += (' > ' + outfile_name)

    return cmd_str

  def _compute_column_indices(self):
    """Return a hash of column indices keyed by column name."""
    return { c:i for (i,c) in enumerate(self.column_names) }
  
