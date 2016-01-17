from ordered_set import OrderedSet
from collections import Iterable
import re

import ast
import sympy as sp

from column import ColumnName, InvalidColumnNameError
from boolean import And, Or
from sql_tokenizer import stringify_conditions

def get_cnf_conditions(conditions):
    tree = ast.parse(stringify_conditions(conditions))
    cp = ConditionParser()
    cp.visit(tree)
    return cp.get_expressions(cp.conditions, cp.expression_symbols)


class ConditionParser(ast.NodeVisitor):
    # identify any join-capable RelationalExpression; these are REs that are not Ors, that do not
    #       have NOTs; throw these join-capable into a list
    # --> PLAN <--
    # make a { tablealias: [RE, RE, RE, ... ], ... } index of REs available for joins
    # step through the plan sequence and assign REs as soon as their two tables exist

    def __init__(self, *args, **kwargs):
        self.conditions = []
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
        boolean_op = self.boolean_ops_map[node.op.__class__]

        self.ops_stack.append({'op': boolean_op, 'args': []})

        for value in node.values:
            self.visit(value)

        this_op = self.ops_stack.pop()

        if self.ops_stack:
            self.ops_stack[-1]['args'].append(this_op['op'](*this_op['args']))
        else:
            self.conditions.append(this_op['op'](*this_op['args']))

    def visit_Compare(self, node):

        if isinstance(node.left, ast.Attribute):
            left = node.left.value.id + '.' + node.left.attr
        elif isinstance(node.left, ast.Name):
            left = node.left.id
        else:
            left = node.left.n

        if isinstance(node.left, ast.Attribute):
            right = node.comparators[0].value.id + '.' + node.left.attr
        elif isinstance(node.comparators[0], ast.Name):
            right = node.comparators[0].id
        else:
            right = node.comparators[0].n
        operator = self.relational_ops_map[node.ops[0].__class__]

        expr = Expression(left, operator, right)
        expr_symbol = sp.Symbol(str(expr))
        self.expression_symbols[expr_symbol] = expr

        if self.ops_stack:
            self.ops_stack[-1]['args'].append(expr_symbol)
        else:
            self.conditions.append(expr_symbol)

    @property
    def cnf_conditions(self):
        cnf = sp.to_cnf(self.conditions)
        return self.get_expressions(cnf, self.expression_symbols)

    @classmethod
    def get_expressions(cls, conditions, expression_symbols):

        expressions = []
        for arg in conditions:
            if isinstance(arg, sp.Symbol):
                expressions.append(expression_symbols[arg])
            elif isinstance(arg, sp.Or):
                or_expressions = Or(cls.get_expressions(arg))
                expressions.append(or_expressions)
            else:
                raise Exception("Conditions aren't in CNF: {}".format(conditions))
                
        return And(expressions)


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

def negate_operator(operator):

    negations = {
        '==': '!=',
        '>=': '<',
        '<=': '>'
    }
    negations = dict( negations.items() + [ (op[1], op[0], ) for op in negations.items() ] )

    return negations[operator]

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

    def negate(self):
        self.operator = negate_operator(self.operator)

    def __str__(self):
        return 'Expression({})'.format(
            ' '.join([repr(self.left_operand), self.operator, repr(self.right_operand)])
        )

    def __repr__(self):
        return str(self)

