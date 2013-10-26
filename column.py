class Column:
  """Identifies a single named column of data, either by column name only or qualified by the
  table name on which it appears. Columns are case-insensitive.
  """

  def __init__(self, token_string):

    token_string_parts = token_string.split('.')
    self.name = token_string_parts[-1]
    self.table_name = ''
    self._ancestor_table_names = []

    # assign a table name if there is one
    if len(token_string_parts) == 2:
      self.table_name = token_string_parts[0] 

  @property
  def name(self):
    return self._name

  @name.setter
  def name(self, value):
    self._cased_name = value
    self._name = value.upper()

  @property
  def table_name(self):
    return self._table_name

  @table_name.setter
  def table_name(self, value):
    self._cased_table_name = value
    self._table_name = value.upper()

  @property
  def ancestor_table_names(self):
    return self._ancestor_table_names

  def __eq__(self, other):
    if type(other) is type(self):
      return (self.name == other.name and self.table_name == other.table_name)
    return False

  def __hash__(self):
    # ignore cased names that may have been used for initialization
    return (hash(self.name) ^ hash(self.table_name))

  def __str__(self):
    return self._cased_name

  def __repr__(self):
    if self.table_name:
      return '.'.join([self.table_name, self.name])
    return self.name

  def name_match(self, columns_to_match):
    """Return a list of columns with the same column name."""
    return [col for col in columns_to_match if col.name == self.name]

  def qualified_match(self, columns_to_match):
    """Return a list of columns with the same column name and table name."""
    return [col for col in columns_to_match if col == self]

  def matching(self, columns_to_match):
    """Return a list of columns that match this one."""

    if self.table_name:
      return self.qualified_match(columns_to_match)

    return self.name_match(columns_to_match)