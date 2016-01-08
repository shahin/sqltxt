import unittest
import os
from sqltxt.table import Table 
from sqltxt.column import Column, ColumnName
from sqltxt.plan import build_graph, add_reverse_edges_to_graph, traverse, is_reachable

class PlanTest(unittest.TestCase):


    def test_traverse(self):
        graph = {
            'a': { 'idx': 1, 'neighbors': set([frozenset(['b']), frozenset(['c'])]), },
            'b': { 'idx': 2, 'neighbors': set([frozenset(['a'])]), },
            'c': { 'idx': 3, 'neighbors': set([frozenset(['a'])]), },
        }

    def test_build_graph(self):
        join_list = [
            { 'relation': {'path': 'a.txt', 'alias': 'a'}, },
            {
                'relation': {'path': 'b.txt', 'alias': 'b'},
                'join_conditions': [{
                    'left_operand': ColumnName('a.col1'),
                    'right_operand': ColumnName('b.col1')
                    }]
            },
            {
                'relation': {'path': 'c.txt', 'alias': 'c'},
                'join_conditions': [{
                    'left_operand': ColumnName('a.col1'),
                    'right_operand': ColumnName('c.col1')
                    }]
            }
        ]

        expected_graph = {
            'a': { 'idx': 0, 'neighbors': set([frozenset(['b']), frozenset(['c'])]), },
            'b': { 'idx': 1, 'neighbors': set([frozenset(['a'])]), },
            'c': { 'idx': 2, 'neighbors': set([frozenset(['a'])]), },
        }

        actual_graph = build_graph(join_list)
        self.assertEqual(actual_graph, expected_graph)

    def test_add_reverse_edges_to_graph(self):
        actual_graph = {
            'a': { 'idx': 0, 'neighbors': set([]), },
            'b': { 'idx': 1, 'neighbors': set([frozenset(['a'])]), },
            'c': { 'idx': 2, 'neighbors': set([frozenset(['a'])]), },
        }
        expected_graph = {
            'a': { 'idx': 0, 'neighbors': set([frozenset(['b']), frozenset(['c'])]), },
            'b': { 'idx': 1, 'neighbors': set([frozenset(['a'])]), },
            'c': { 'idx': 2, 'neighbors': set([frozenset(['a'])]), },
        }

        add_reverse_edges_to_graph(actual_graph)
        self.assertEqual(actual_graph, expected_graph)

        actual_graph = {
            'a': { 'idx': 0, 'neighbors': set([]), },
            'b': { 'idx': 1, 'neighbors': set([frozenset(['a'])]), },
            'c': { 'idx': 2, 'neighbors': set([frozenset(['a', 'b'])]), },
            'd': { 'idx': 3, 'neighbors': set([frozenset(['c'])]), },
        }
        expected_graph = {
            'a': { 'idx': 0, 'neighbors': set([frozenset(['b']), frozenset(['c'])]), },
            'b': { 'idx': 1, 'neighbors': set([frozenset(['a']), frozenset(['c'])]), },
            'c': { 'idx': 2, 'neighbors': set([frozenset(['a', 'b'])]), },
        }


    def test_is_reachable(self):
        graph = {
            'a': { 'idx': 0, 'neighbors': set([frozenset(['b']), frozenset(['c'])]), },
            'b': { 'idx': 1, 'neighbors': set([frozenset(['a'])]), },
            'c': { 'idx': 2, 'neighbors': set([frozenset(['a']), frozenset(['d'])]), },
            'd': { 'idx': 2, 'neighbors': set([frozenset(['c'])]), },
        }

        self.assertFalse(is_reachable('d', graph, {'a': 0}))
        self.assertTrue(is_reachable('d', graph, {'c': 0}))
        self.assertTrue(is_reachable('d', graph, {'a': 0, 'c': 1}))

    def test_is_not_reachable_when_incomplete_neighbor_sets_are_visited(self):
        graph = {
            'a': { 'idx': 0, 'neighbors': set([frozenset(['b']), frozenset(['c'])]), },
            'b': { 'idx': 1, 'neighbors': set([frozenset(['a']), frozenset(['c'])]), },
            'c': { 'idx': 2, 'neighbors': set([ frozenset(['a', 'b']) ]), },
        }

        self.assertFalse(is_reachable('d', graph, {'a': 0}))
        self.assertTrue(is_reachable('d', graph, {'c': 0}))
        self.assertTrue(is_reachable('d', graph, {'a': 0, 'c': 1}))

