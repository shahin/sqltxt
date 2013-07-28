class Table:

  def __init__(self, name, delimiter=',', extension='txt', cmd=None, column_names=None):
    self.name = name
    self.extension = extension
    self.delimiter = delimiter

    if cmd is None:
      self.cmd = "cat {0}.{1} > {2}.{1}".format(name, extension, id(self))
      self.column_names = self._parse_column_names()
    else:
      self.cmd = cmd
      self.column_names = column_names

    self.column_idxs = self._compute_column_indices()
    
  def _parse_column_names(self):

    with open(self.name + '.' + self.extension) as table_file:
      head = table_file.readline()

    return head.split(self.delimiter)

  def _compute_column_indices(self):
    return { c.upper():i for (i,c) in enumerate(self.column_names) }

  
