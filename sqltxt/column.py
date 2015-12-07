from ordered_set import OrderedSet

class Column(object):
    """Identifies a single named column of data, either by column name only or qualified by the
    table name on which it appears. Columns are case-insensitive.
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
     
    def __eq__(self, other):
        if type(other) is type(self):
            return (
                self.name == other.name and
                ((other.qualifiers & self.qualifiers) or
                    not other.qualifiers or
                    not self.qualifiers
                )
            )
        return False

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

