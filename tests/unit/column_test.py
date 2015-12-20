import unittest
from sqltxt.column import ColumnName, Column, merge_columns

class ColumnTest(unittest.TestCase):

    def test_columns_with_the_same_name_are_equal(self):
        self.assertEqual(ColumnName('col_a'), ColumnName('col_a'))

    def test_columns_with_different_names_are_not_equal(self):
        self.assertNotEqual(ColumnName('col_a'), ColumnName('col_b'))

    def test_columns_with_the_same_name_and_same_qualifier_are_equal(self):
        self.assertEqual(ColumnName('table_a.col_a'), ColumnName('table_a.col_a'))

    def test_columns_with_the_same_name_and_different_qualifiers_are_not_equal(self):
        self.assertNotEqual(ColumnName('table_b.col_a'), ColumnName('table_a.col_a'))

    def test_columns_with_different_names_and_the_same_qualifier_are_not_equal(self):
        self.assertNotEqual(ColumnName('table_a.col_a'), ColumnName('table_a.col_b'))

    def test_columns_with_the_same_name_but_different_cases_are_equal(self):
        self.assertEqual(ColumnName('Col_A'), ColumnName('col_a'))
        self.assertEqual(ColumnName('Table_A.col_a'), ColumnName('table_a.COL_A'))

    def test_unqualified_columns_are_greater_than_qualified_columns_with_the_same_name(self):
        col_a_unqualified = ColumnName('col_a')
        col_a_qualified = ColumnName('table_a.col_a')
        self.assertGreater(col_a_unqualified, col_a_qualified)
        self.assertLess(col_a_qualified, col_a_unqualified)
        self.assertNotEqual(col_a_qualified, col_a_unqualified)

        col_a_unqualified = ColumnName('col_a')
        col_a_qualified = ColumnName('table_a.col_a', qualifiers=['ta'])
        self.assertGreater(col_a_unqualified, col_a_qualified)
        self.assertLess(col_a_qualified, col_a_unqualified)
        self.assertNotEqual(col_a_qualified, col_a_unqualified)

        col_a_unqualified = ColumnName('col_a')
        col_a_qualified = ColumnName('col_a', qualifiers=['table_a', 'ta'])
        self.assertGreater(col_a_unqualified, col_a_qualified)
        self.assertLess(col_a_qualified, col_a_unqualified)
        self.assertNotEqual(col_a_qualified, col_a_unqualified)

    def test_columns_are_greater_than_columns_with_a_superset_of_their_qualifiers(self):
        col_a_1 = ColumnName('col_a', qualifiers=['ta'])
        col_a_2 = ColumnName('col_a', qualifiers=['table_a', 'ta'])
        self.assertGreater(col_a_1, col_a_2)

    def test_qualified_columns_without_intersecting_qualifiers_are_not_equal(self):
        col_a_unqualified = ColumnName('col_a', qualifiers=['table_b', 'tb'])
        col_a_qualified = ColumnName('col_a', qualifiers=['table_a', 'ta'])
        self.assertNotEqual(col_a_unqualified, col_a_qualified)
    
    def test_column_with_multiple_names_matches_on_both(self):
        col_a = Column('left.col_a')
        col_b = Column('right.col_b')
        merged = merge_columns(col_a, col_b)

        self.assertTrue(merged.match(col_a))
        self.assertTrue(merged.match(col_b))
        self.assertFalse(merged.match(Column('right.col_a')))

