import unittest
from sqltxt.column import Column 

class ColumnTest(unittest.TestCase):

    def test_columns_with_the_same_name_are_equal(self):
        self.assertEqual(Column('col_a'), Column('col_a'))

    def test_columns_with_different_names_are_not_equal(self):
        self.assertNotEqual(Column('col_a'), Column('col_b'))

    def test_columns_with_the_same_name_and_same_qualifier_are_equal(self):
        self.assertEqual(Column('table_a.col_a'), Column('table_a.col_a'))

    def test_columns_with_the_same_name_and_different_qualifiers_are_not_equal(self):
        self.assertNotEqual(Column('table_b.col_a'), Column('table_a.col_a'))

    def test_columns_with_different_names_and_the_same_qualifier_are_not_equal(self):
        self.assertNotEqual(Column('table_a.col_a'), Column('table_a.col_b'))

    def test_columns_with_the_same_name_but_different_cases_are_equal(self):
        self.assertEqual(Column('Col_A'), Column('col_a'))
        self.assertEqual(Column('Table_A.col_a'), Column('table_a.COL_A'))

    def test_qualified_and_unqualified_columns_with_the_same_name_are_equal(self):
        col_a_unqualified = Column('col_a')
        col_a_qualified = Column('table_a.col_a')
        self.assertEqual(col_a_unqualified, col_a_qualified)

        col_a_unqualified = Column('col_a')
        col_a_qualified = Column('table_a.col_a', qualifiers=['ta'])
        self.assertEqual(col_a_unqualified, col_a_qualified)

        col_a_unqualified = Column('col_a')
        col_a_qualified = Column('col_a', qualifiers=['table_a', 'ta'])
        self.assertEqual(col_a_unqualified, col_a_qualified)

    def test_qualified_columns_with_intersecting_qualifiers_are_equal(self):
        col_a_unqualified = Column('col_a', qualifiers=['ta'])
        col_a_qualified = Column('col_a', qualifiers=['table_a', 'ta'])
        self.assertEqual(col_a_unqualified, col_a_qualified)

        col_a_unqualified = Column('col_a', qualifiers=['ta', 'tb'])
        col_a_qualified = Column('col_a', qualifiers=['table_a', 'ta'])
        self.assertEqual(col_a_unqualified, col_a_qualified)

    def test_qualified_columns_without_intersecting_qualifiers_are_not_equal(self):
        col_a_unqualified = Column('col_a', qualifiers=['table_b', 'tb'])
        col_a_qualified = Column('col_a', qualifiers=['table_a', 'ta'])
        self.assertNotEqual(col_a_unqualified, col_a_qualified)
