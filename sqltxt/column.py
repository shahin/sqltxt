from ordered_set import OrderedSet
import copy

class Column(object):

    def __init__(self, name, qualifiers=None):
        self.names = [ColumnName(name, qualifiers)]
        self.alias = self.names[0]

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, column_name):
        if not(column_name.match(*self.names)):
            self.add_name(column_name)
        self._alias = column_name

    def add_name(self, name, qualifiers=None):
        self.names.append(ColumnName(name, qualifiers))

    def match(self, *right_columns):
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

    def __init__(self, name, qualifiers=None):
        name_parts = name.split('.')
        self.name = name_parts[-1]
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
        if self.qualifiers:
            return '<ColumnName ' + '.'.join([qualifiers_to_str(self.qualifiers), self.name]) + '>'
        return self.name

    def match(self, *right_column_names):
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
