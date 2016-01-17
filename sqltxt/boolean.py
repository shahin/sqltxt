"""This module defines containers used to identify the boolean operator for the contained
expressions.
"""

import collections

class BooleanOperator(collections.MutableSequence):
    """A base class for boolean binary operators ('and' and 'or')."""

    operator_str = 'NotImplemented'

    def __init__(self, args):
        self.args = args

    def args_with_operator(self):
        return zip(self.args, [self.operator_str] * len(self.args))[:-1]

    def __str__(self):
        return ' '.join([str(a) for a in self.args_with_operator()])

    def __getitem__(self, key):
        return self.args[key]

    def __setitem__(self, key, value):
        self.args[key] = value

    def __delitem__(self, key):
        del self.args[key]

    def __len__(self):
        return len(self.args)

    def __str__(self):
        return 'And({})'.format(self.args)

    def __repr__(self):
        return str(self)

    def insert(self, i, x):
        self.args.insert(i, x)

class Or(BooleanOperator):
    operator_str = 'or'

class And(BooleanOperator):
    operator_str = 'and'
