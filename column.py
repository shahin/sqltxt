class Column:
  """Identifies a single named column of data, either by column name only or qualified by the
  table name on which it appears. Columns are case-insensitive.
  """

  def __init__(self, token_string, alias = None, ancestors = []):

    token_string_parts = token_string.split('.')
    self.name = token_string_parts[-1]
    self.table_name = ''
    self.ancestors = ancestors

    # assign a table name if there is one
    if len(token_string_parts) == 2:
      self.table_name = token_string_parts[0] 

    self.alias = alias
    if self.alias is None:
      self.alias = self._name

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

  def ancestor_match(self, columns_to_match):
    """Return a list of columns with an ancestor that has the same column name and table 
    name."""
    return [col for col in columns_to_match if self.match(col.ancestors)]

  def match(self, columns_to_match, include_ancestors = False):
    """Return a list of columns that match this one.

    If this column is qualified, require a match on table name and column name. Otherwise,
    require a match on column name only.
    
    """

    matching_columns = []

    if self.table_name:
      matching_columns.extend(self.qualified_match(columns_to_match))
      if include_ancestors:
        matching_columns.extend(self.ancestor_match(columns_to_match))
    else:
      matching_columns.extend(self.name_match(columns_to_match))

    return matching_columns
