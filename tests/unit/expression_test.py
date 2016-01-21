import unittest

from sqltxt.expression import Expression, And, Or
from sqltxt.column import ColumnName

class ExpressionTest(unittest.TestCase):

    def test_expression_column_names(self):
        expr = Expression('a.a', '==', 'b.b')
        expected_column_names = set([ColumnName('a.a'), ColumnName('b.b')])
        self.assertEqual(expected_column_names, expr.column_names)

        expr = Expression('a.a', '==', '1')
        expected_column_names = set([ColumnName('a.a')])
        self.assertEqual(expected_column_names, expr.column_names)

    def test_condition_column_names(self):
        boolean_condition = And([
            Expression('a.a', '==', 'b.b'),
            Expression('a.a', '==', '1'),
            Expression('1', '==', 'c.c'),
            Expression('d.d', '==', '1'),
        ])
        expected_column_names = set([
            ColumnName('a.a'), ColumnName('b.b'), ColumnName('c.c'), ColumnName('d.d')
        ])
        self.assertEqual(expected_column_names, boolean_condition.column_names())

    def test_nested_condition_column_names(self):
        boolean_condition = And([
            Or([
                Expression('a.a', '==', 'b.b'),
                Expression('a.a', '==', '1'),
            ]),
            Expression('1', '==', 'c.c'),
            Expression('d.d', '==', '1'),
        ])
        expected_column_names = set([
            ColumnName('a.a'), ColumnName('b.b'), ColumnName('c.c'), ColumnName('d.d')
        ])
        self.assertEqual(expected_column_names, boolean_condition.column_names())
    
        boolean_condition = And([
            Or([
                Expression('a.a', '==', 'b.b'),
                Expression('1', '==', 'c.c'),
            ]),
            Expression('a.a', '==', '1'),
            Expression('d.d', '==', '1'),
        ])
        expected_column_names = set([
            ColumnName('a.a'), ColumnName('b.b'), ColumnName('c.c'), ColumnName('d.d')
        ])
        self.assertEqual(expected_column_names, boolean_condition.column_names())
