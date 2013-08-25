class Table:

  def __init__(self, name, delimiter=',', extension='txt', cmd=None, column_names=None):

    self.name = name
    self.extension = extension
    self.delimiter = delimiter
    self.sorted_by = []

    # initialize as a non-materialized table
    self.is_file = False
    self.cmds = [cmd]
    self.outfile_name = "{0}.{1}.out".format(name, extension)
    self.column_names = column_names

    if cmd is None:
      # initialize as a materialized table
      self.cmds = []
      self.outfile_name = "{0}.{1}".format(name, extension)
      self.column_names = self._parse_column_names()
      self.is_file = True

    self.column_idxs = self._compute_column_indices()

  def order_columns(self, col_names_in_order, drop_other_columns = False):
    
    reordered_col_idxs = [self.column_idxs[col_name] for col_name in col_names_in_order]
    unchanged_col_idxs = [
      self.column_idxs[col_name] for col_name in self.column_names
      if col_name not in col_names_in_order
      ]

    col_idxs = reordered_col_idxs
    if not drop_other_columns:
      col_idxs += unchanged_col_idxs

    reorder_cmd = 'cut -d{0} -f{1}'.format(self.delimiter, ','.
      join(str(idx) for idx in col_idxs))

    self.column_names = [self.column_names[idx-1] for idx in col_idxs]
    self.column_idxs = self._compute_column_indices()
    self.cmds.append(reorder_cmd)

  def sort(self, col_names_to_sort_by):

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

    sort_cmd = 'sort -t{0} -k {1}'.format(self.delimiter, ','.
      join(str(idx) for idx in column_idxs_to_sort_by))
    self.sorted_by = col_names_to_sort_by
    self.cmds.append(sort_cmd)
    

  def select_subset(self, conditions):

    # translate a list of boolean conditions to awk syntax
    condition_str = ''
    for expr_part in conditions:
      if expr_part == 'and':
        condition_str += ' && '
      elif expr_part == 'or':
        condition_str += ' || '
      else:
        expr_part = [ ('$' + str(self.column_idxs[token]) 
            if token in self.column_idxs else token) 
            for token in expr_part ]
        condition_str += ' '.join(expr_part)

    # treat no conditions as an always-true condition
    if condition_str == '':
      condition_str = '1'

    columns = ','.join(['$' + str(self.column_idxs[c]) for c in self.column_names])
    awk_cmd = "awk -F'{0}' 'OFS=\"{0}\" {{ if ({1}) {{ print {2} }} }}'".format(
      self.delimiter, condition_str, columns)
    self.cmds.append(awk_cmd)

  def get_cmd_str(self, outfile_name = None):

    cmds = self.cmds

    # add input piped from a file
    if self.is_file:
      cmds = ['tail +2 {0}.{1}'.format(self.name, self.extension)] + cmds 

    cmd_str = ' | '.join(cmds)

    # add output redirection to file
    if outfile_name is not None:
      cmd_str += (' > ' + outfile_name)

    return cmd_str

  def get_cmd_to_pipe_str(self):

    pipe_name = self.get_pipe_name() 
    create_pipe_cmd = 'mkfifo {0}; '.format(pipe_name)
    return create_pipe_cmd + self.get_cmd_str(outfile_name = pipe_name)

  def get_pipe_name(self):

    return self.name + '.fifo'

  def _parse_column_names(self):

    with open(self.name + '.' + self.extension) as table_file:
      head = table_file.readline().rstrip()

    return head.split(self.delimiter)

  def _compute_column_indices(self):
    return { c:i+1 for (i,c) in enumerate(self.column_names) }
  
