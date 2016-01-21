import unittest
import os
from sqltxt.table import Table 
from sqltxt.column import Column, ColumnName
from sqltxt.plan import build_graph, traverse, classify_conditions

class PlanTest(unittest.TestCase):

    def test_classify_conditions(self):
        aliases = { 'a': 'a', 'alpha': 'a', 'b': 'b', 'c': 'c' }
        conditions = [
            [{ 'left_operand': ColumnName('a.col1'), 'right_operand': ColumnName('b.col1') }],
            [{ 'left_operand': ColumnName('a.col1'), 'right_operand': 5 }],
            [{ 'left_operand': ColumnName('a.col1'), 'right_operand': ColumnName('c.col1') }],
            [{ 'left_operand': ColumnName('a.col1'), 'right_operand': ColumnName('alpha.col2') }],
        ]

        expected_join_conditions = [
            [{ 'left_operand': ColumnName('a.col1'), 'right_operand': ColumnName('b.col1') }],
            [{ 'left_operand': ColumnName('a.col1'), 'right_operand': ColumnName('c.col1') }],
        ]
        expected_where_conditions = [
            [{ 'left_operand': ColumnName('a.col1'), 'right_operand': 5 }],
            [{ 'left_operand': ColumnName('a.col1'), 'right_operand': ColumnName('alpha.col2') }],
        ]

        actual_join_conditions, actual_where_conditions = classify_conditions(conditions, aliases)
        self.assertEqual(actual_join_conditions, expected_join_conditions)
        self.assertEqual(actual_where_conditions, expected_where_conditions)

    def test_build_graph(self):
        relations = [
            {'path': 'a.txt', 'alias': 'a'},
            {'path': 'b.txt', 'alias': 'b'},
            {'path': 'c.txt', 'alias': 'c'},
        ]
        aliases = { 'a': 'a', 'alpha': 'a', 'b': 'b', 'c': 'c' }
        conditions = [
            [{ 'left_operand': ColumnName('a.col1'), 'right_operand': ColumnName('b.col1') }],
            [{ 'left_operand': ColumnName('a.col1'), 'right_operand': ColumnName('c.col1') }],
        ]

        expected_graph = {
            'a': { 'idx': 0, 'neighbors': set([frozenset(['b']), frozenset(['c'])]), },
            'b': { 'idx': 1, 'neighbors': set([frozenset(['a'])]), },
            'c': { 'idx': 2, 'neighbors': set([frozenset(['a'])]), },
        }

        actual_graph = build_graph(relations, conditions, aliases)
        self.assertEqual(actual_graph, expected_graph)

    def test_traverse(self):

        graph = {
            'a': { 'idx': 0, 'neighbors': set([frozenset(['b']), frozenset(['c'])]), },
            'b': { 'idx': 1, 'neighbors': set([frozenset(['a'])]), },
            'c': { 'idx': 2, 'neighbors': set([frozenset(['a'])]), },
        }
        priorities = { 'a': 15, 'b': 20, 'c': 3 }

        actual_node_order = traverse(graph, priorities)
        expected_node_order = { 'a': 1, 'b': 0, 'c': 2 }
        self.assertEqual(actual_node_order, expected_node_order)

        priorities = { 'a': 25, 'b': 20, 'c': 3 }

        actual_node_order = traverse(graph, priorities)
        expected_node_order = { 'a': 0, 'b': 1, 'c': 2 }
        self.assertEqual(actual_node_order, expected_node_order)
