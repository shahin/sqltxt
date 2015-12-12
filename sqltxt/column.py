from ordered_set import OrderedSet

class Column(object):
    """Identifies a single named column of data, either by column name only or qualified by the
    table name on which it appears. Columns are case-insensitive.

    Two equal Columns always have the same hash value, but Columns with the same hash value are
    not necessarily equal. This allows us to look up a qualified Column key in a dictionary using
    an unqualified Column.
    """

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
     
    def __lt__(self, other):
        if type(other) is type(self):
            return (
                self.name == other.name and
                (len(self.qualifiers) > 0 and len(other.qualifiers) == 0)
            )
        return False
     
    def __gt__(self, other):
        if type(other) is type(self):
            return (
                self.name == other.name and
                (len(self.qualifiers) == 0 and len(other.qualifiers) > 0)
            )
        return False

    def __eq__(self, other):
        # TODO: change this since now table_a.col_a == col_a == table_b.col_a, but
        # table_a.col_a != table_b.col_a
        # I think this means we have to abandon the dictionary interface, create a match() function
        if type(other) is type(self):
            return (
                self.name == other.name and
                (
                    (other.qualifiers & self.qualifiers) or
                    (not other.qualifiers and not self.qualifiers)
                )
            )
        return False

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
            return '.'.join([qualifiers_to_str(self.qualifiers), self.name])
        return self.name

def qualifiers_to_str(qualifiers):
    if len(qualifiers) == 0:
        return ''
    elif len(qualifiers) == 1:
        return qualifiers[0]
    else:
        return '(' + '|'.join(sorted(qualifiers)) + ')'

def get_equivalent_columns(columns):
    """Given a list of columns, return a list of lists of equal columns."""
    equivalence_classes = [[columns[0]]]
    for col in columns:
        for equivalence_class in equivalence_classes:
            if col == equivalence_class[0]:
                equivalence_class.append(col)
                break 
            equivalence_classes.append([col])
    
    return equivalence_classes



