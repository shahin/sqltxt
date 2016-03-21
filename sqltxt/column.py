from ordered_set import OrderedSet
import copy
import re

VALID_IDENTIFIER_REGEX = '^[a-zA-Z_][a-zA-Z0-9_.]*$'

def is_valid_identifier(identifier):
    return re.match(VALID_IDENTIFIER_REGEX, identifier)


class AmbiguousColumnNameError(Exception):
    def __init__(self, column_name, matched_columns):
        message = 'Ambiguous column reference {0} which matches {1}'.format(
            column_name, [c.names for c in matched_columns])
        super(self.__class__, self).__init__(message)


class UnknownColumnNameError(Exception):
    def __init__(self, column_name):
        message = 'Unknown column name {0}'.format(column_name.original_token)
        super(self.__class__, self).__init__(message)


class InvalidColumnNameError(Exception):
    def __init__(self, column_name):
        message = 'Invalid column name {0}'.format(column_name)
        super(self.__class__, self).__init__(message)


class Column(object):
    """A Column instance represents a column of data in a table. It manages the list of ColumnNames
    that refer to it.
    
    Only one of these ColumnNames is used on output. This is called the Column's alias."""

    def __init__(self, name, qualifiers=None):
        self.names = [ColumnName(name, qualifiers)]
        self.alias = self.names[0]

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, column_name):
        """The alias is the ColumnName that should be used on output."""
        if not(column_name.match(*self.names)):
            self.add_name(column_name)
        self._alias = column_name

    def add_name(self, name, qualifiers=None):
        self.names.append(ColumnName(name, qualifiers))

    def match(self, *right_columns):
        """Given one or more Columns "on the right" to match against this Column, return the subset
        of those columns that match this Column on at least one name."""
        matches = []
        for left_name in self.names:
            for col in right_columns:
                if left_name.match(*col.names):
                    matches.append(col)
        return matches

    def __eq__(self, other):
        return self.names == other.names

    def __str__(self):
        return str(self.alias)

    def __repr__(self):
        return '<Column ' + '|'.join([repr(n) for n in self.names]) + '>'


class ColumnName(object):
    """A ColumnName is a string naming the Column and optionally a set of qualifiers.
    
    In SQL, ColumnName qualifiers are usually table names or table aliases."""

    def __init__(self, name, qualifiers=None):
        self.original_token = name
        name_parts = name.split('.')
        self.name = name_parts[-1]

        if not is_valid_identifier(self.name):
            raise InvalidColumnNameError(self.name)

        self.qualifiers = OrderedSet(qualifiers or [])
        if len(name_parts) > 1:
            self.qualifiers.add('.'.join(name_parts[:-1]).lower())

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._cased_name = value
        self._name = value.lower()

    @property
    def qualifiers(self):
        return self._qualifiers

    @qualifiers.setter
    def qualifiers(self, value):
        self._qualifiers = OrderedSet([qual.lower() for qual in value])

    def __eq__(self, other):
        if type(other) is type(self):
            return (self.name == other.name and other.qualifiers == self.qualifiers)
        return False
     
    def __gt__(self, other):
        if type(other) is type(self):
            return (self.name == other.name and self.qualifiers < other.qualifiers)
        return False
     
    def __lt__(self, other):
        return other > self

    def __ge__(self, other):
        return (self > other or self == other)

    def __le__(self, other):
        return (self < other or self == other)

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return self._cased_name

    def __repr__(self):
        return '<ColumnName ' + '.'.join([qualifiers_to_str(self.qualifiers), self.name]) + '>'

    def match(self, *right_column_names):
        """Given a list of ColumnNames, return a list of those that match this ColumName.

        This operation is not commutative. That is, A.match(B) =/=> B.match(A)."""
        return [col for col in right_column_names if self >= col]

def qualifiers_to_str(qualifiers):
    if len(qualifiers) == 0:
        return ''
    else:
        return '(' + '|'.join(sorted(qualifiers)) + ')'

def merge_columns(left_column, right_column):
    merged = copy.deepcopy(left_column)
    for col_name in right_column.names:
        merged.add_name(col_name.name, col_name.qualifiers)
    return merged
