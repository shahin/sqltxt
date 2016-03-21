import collections
import re

import ast
import sympy as sp
from sympy.logic import boolalg

from column import ColumnName, InvalidColumnNameError
from sql_tokenizer import stringify_conditions

def get_cnf_conditions(conditions):
    tree = ast.parse(stringify_conditions(conditions))
    cp = ConditionParser()
    cp.visit(tree)
    return cp.cnf_conditions


class ConditionParser(ast.NodeVisitor):

    def __init__(self, *args, **kwargs):
        self.conditions = None
        self.ops_stack = []
        self.boolean_ops_map = { ast.Or: sp.Or, ast.And: sp.And, }
        self.relational_ops_map = {
            ast.Eq : '==',
            ast.Gt : '>',
            ast.GtE : '>=',
            ast.Lt : '<',
            ast.LtE : '<=',
            ast.NotEq : '!=',
        }
        self.expression_symbols = {}
        super(self.__class__, self).__init__(*args, **kwargs)

    def visit_BoolOp(self, node):
        """Given a BoolOp node in the AST, translate it to a SymPy operator, stash it
        in a dictionary with its arguments and append the dictionary to the stack."""
        boolean_op = self.boolean_ops_map[node.op.__class__]

        self.ops_stack.append({'op': boolean_op, 'args': []})

        for value in node.values:
            self.visit(value)

        this_op = self.ops_stack.pop()

        if self.ops_stack:
            self.ops_stack[-1]['args'].append(this_op['op'](*this_op['args']))
        else:
            self.conditions = this_op['op'](*this_op['args'])

    def visit_Compare(self, node):

        expr_operands = []
        for child_node in (node.left, node.comparators[0], ):
            if isinstance(child_node, ast.Attribute):
                operand = self._get_attribute_name(child_node)
            elif isinstance(child_node, ast.Name):
                operand = child_node.id
            elif isinstance(child_node, ast.Str):
                operand = '"' + child_node.s + '"'
            else:
                operand = child_node.n
            expr_operands.append(operand)

        operator = self.relational_ops_map[node.ops[0].__class__]

        expr = Expression(expr_operands[0], operator, expr_operands[1])
        expr_symbol = sp.Symbol(str(expr))
        self.expression_symbols[expr_symbol] = expr

        if self.ops_stack:
            self.ops_stack[-1]['args'].append(expr_symbol)
        else:
            self.conditions = expr_symbol

    def _get_attribute_name(self, node):
        if isinstance(node, ast.Attribute):
            return self._get_attribute_name(node.value) + '.' + node.attr
        elif isinstance(node, ast.Name):
            return node.id

    @property
    def cnf_conditions(self):
        cnf = sp.to_cnf(self.conditions)
        cnf_expressions = self.get_expressions(cnf, self.expression_symbols)
        if not isinstance(cnf_expressions, AndList):
            cnf_expressions = AndList([cnf_expressions])

        return cnf_expressions

    @classmethod
    def get_expressions(cls, conditions, expression_symbols):
        """Given conditions as symbols in CNF, return those same conditions as Expressions in a
        list."""

        expressions = []
        if isinstance(conditions, sp.Symbol):
            expressions.append(expression_symbols[conditions])
        elif isinstance(conditions, boolalg.BooleanFunction):
            for arg in conditions.args:
                if isinstance(arg, sp.Symbol):
                    expressions.append(expression_symbols[arg])
                elif isinstance(arg, sp.Or):
                    or_expressions = OrList(cls.get_expressions(arg, expression_symbols))
                    expressions.append(or_expressions)
                else:
                    raise Exception("Conditions aren't in CNF: {}".format(conditions))
                
        if isinstance(conditions, sp.Or):
            return_wrapper = OrList
        else:
            return_wrapper = AndList
        return return_wrapper(expressions)


def is_boolean_operator(term):
    return term.lower() in ('and', 'or', )

def is_relational_operator(term):
    try:
        normalize_relational_operator(term)
        return True
    except InvalidExpressionOperator:
        return False


def normalize_relational_operator(operator):

    comparison_operators = {
        '=': '==',
        'eq': '==',
        'ne': '!=',
        'ge': '>=',
        'gt': '>',
        'le': '<=',
        'lt': '<'
    }

    if operator in comparison_operators:
        return comparison_operators[operator]
    elif operator in comparison_operators.values():
        return operator
    else:
        raise InvalidExpressionOperator(operator)

class InvalidExpressionOperator(Exception):

    def __init__(self, operator):
        self.operator = operator
        message = "Invalid operator '{}'".format(operator)
        super(self.__class__, self).__init__(message)

class Expression(object):

    def __init__(self, left_operand, operator, right_operand):
        self.left_operand = self.normalize_operand(left_operand)
        self.right_operand = self.normalize_operand(right_operand)
        self.operator = normalize_relational_operator(operator)

    @property
    def can_join(self):
        return self.is_across_tables() and self.operator == '=='
    
    @property
    def column_names(self):
        return set([
            cn for cn in (self.left_operand, self.right_operand, ) if isinstance(cn, ColumnName)
        ])

    def as_dict(self):
        return {
            'left_operand': self.left_operand,
            'operator': self.operator,
            'right_operand': self.right_operand
        }

    @staticmethod
    def normalize_operand(operand):
        if isinstance(operand, basestring):
            try:
                return ColumnName(operand)
            except:
                return operand
        else:
            return operand

    def is_across_tables(self):
        if not isinstance(self.left_operand, ColumnName) or not isinstance(self.right_operand, ColumnName):
            return False

        return \
            len(set(self.right_operand.qualifiers) | set(self.left_operand.qualifiers)) > \
            len(set(self.right_operand.qualifiers) & set(self.left_operand.qualifiers))

    def __str__(self):
        return 'Expression({})'.format(
            ' '.join([repr(self.left_operand), self.operator, repr(self.right_operand)])
        )

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return  \
            self.left_operand == other.left_operand and \
            self.operator == other.operator and \
            self.right_operand == other.right_operand

class BooleanExpression(collections.MutableSequence):
    """A base class for boolean binary operators ('and' and 'or')."""

    operator_str = 'NotImplemented'

    def __init__(self, args):
        self.args = args

    @property
    def column_names(self):
        """Return all column names used in the arguments to this boolean operator."""
        column_names = []
        for arg in self.args:
            if isinstance(arg, Expression):
                names = arg.column_names
                if names:
                    column_names.extend(names)
            elif isinstance(arg, BooleanExpression):
                column_names.extend(arg.column_names)
        return set(column_names)

    def args_with_operator(self):
        """Return this boolean expression as a list of Expressions, boolean operators, and nested
        lists for any nested boolean expressions."""
        args = [
            [arg.args_with_operator()] if isinstance(arg, BooleanExpression) else arg
            for arg in self.args
        ]
        return [ i for pair in zip(args, [self.operator_str] * len(self.args)) for i in pair ][:-1]

    def __str__(self):
        return ' '.join([str(a) for a in self.args_with_operator()])

    def __eq__(self, other):
        return \
            all([ left == right for left, right in zip(self.args, other.args) ]) and \
            len(self.args) == len(other.args)

    def __getitem__(self, key):
        return self.args[key]

    def __setitem__(self, key, value):
        self.args[key] = value

    def __delitem__(self, key):
        del self.args[key]

    def __len__(self):
        return len(self.args)

    def __str__(self):
        return '{0}({1})'.format(self.__class__, self.args)

    def __repr__(self):
        return str(self)

    def insert(self, i, x):
        self.args.insert(i, x)


class OrList(BooleanExpression):
    operator_str = 'or'


class AndList(BooleanExpression):
    operator_str = 'and'
