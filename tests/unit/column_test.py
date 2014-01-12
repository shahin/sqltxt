import unittest
from column import Column 

class ColumnTest(unittest.TestCase):

  def test_columns_with_the_same_name_are_equal(self):
    self.assertEqual(Column('col_a'), Column('col_a'))

  def test_columns_with_different_names_are_not_equal(self):
    self.assertNotEqual(Column('col_a'), Column('col_b'))

  def test_columns_with_the_same_name_and_same_table_are_equal(self):
    self.assertEqual(Column('table_a.col_a'), Column('table_a.col_a'))

  def test_columns_with_the_same_name_and_different_tables_are_not_equal(self):
    self.assertNotEqual(Column('table_b.col_a'), Column('table_a.col_a'))

  def test_columns_with_different_names_and_the_same_table_are_not_equal(self):
    self.assertNotEqual(Column('table_a.col_a'), Column('table_a.col_b'))

  def test_columns_with_the_same_name_but_different_cases_are_equal(self):
    self.assertEqual(Column('Col_A'), Column('col_a'))
    self.assertEqual(Column('Table_A.col_a'), Column('table_a.COL_A'))

  def test_column_name_matches_columns_with_the_same_name(self):
    col_a_1 = Column('col_a')
    col_a_2 = Column('col_a')
    self.assertEqual(col_a_1.name_match([col_a_2]), [col_a_2])

  def test_column_name_does_not_match_column_with_different_name(self):
    col_a_1 = Column('col_a')
    col_a_2 = Column('col_b')
    matching_columns = col_a_1.name_match([col_a_2])
    self.assertEqual(col_a_1.name_match([col_a_2]), [])

  def test_column_name_matches_columns_with_the_same_name_and_table(self):
    col_a_1 = Column('col_a')
    col_a_2 = Column('table_a.col_a')
    matching_columns = col_a_1.name_match([col_a_2])
    self.assertEqual(col_a_1.name_match([col_a_2]), [col_a_2])

  def test_column_name_exact_matches_columns_with_the_same_name_and_table(self):
    col_a_1 = Column('table_a.col_a')
    col_a_2 = Column('table_b.col_a')
    col_a_3 = Column('table_a.col_a')
    col_a_4 = Column('col_a')

    matching_columns = col_a_1.qualified_match([col_a_2, col_a_3, col_a_4])
    self.assertEqual(matching_columns, [col_a_3])

  def test_column_matches_columns_with_matching_ancestors(self):
    col_a = Column('table_a.col_a')
    col_a_child = Column('table_a_child.col_a', [col_a])
    col_b = Column('table_b.col_b')

    matching_columns = col_a.ancestor_match([col_a_child, col_b])
    self.assertEqual(matching_columns, [col_a_child])
