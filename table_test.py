import unittest
from table import Table 

class TableTest(unittest.TestCase):

  def setUp(self):
    self.table_a = Table('table_a')

  def test_select_subset(self):
    
    conditions = [['col_b', '==', '2'], 'or', ['col_a', '==', '4']]
    self.table_a.select_subset(conditions)

    cmd_actual = self.table_a.cmd
    cmd_expected = ["awk -F',' 'OFS=\",\" { if ($2 == 2 || $1 == 4) { print $1,$2,$3,$4 } }"]
    self.assertEqual(cmd_actual, cmd_expected)

  def test_reorder(self):

    col_order = ['col_c','col_a']
    self.table_a.order_columns(col_order)

    cmd_actual = self.table_a.cmd
    cmd_expected = ["cut -d, -f3,1,2,4"]
    self.assertEqual(cmd_actual, cmd_expected)

  def test_sort_in_order(self):
    
    sort_by_cols = ['col_a','col_b']
    self.table_a.sort(sort_by_cols)

    cmd_actual = self.table_a.cmd
    cmd_expected = ["sort -t, -k 1,2"]
    self.assertEqual(cmd_actual, cmd_expected)

    self.assertEqual(self.table_a.sorted_by, sort_by_cols)

  def test_sort_out_of_order(self):
    
    sort_by_cols = ['col_c','col_a']
    self.table_a.sort(sort_by_cols)

    cmd_actual = self.table_a.cmd
    cmd_expected = ["cut -d, -f3,1,2,4", "sort -t, -k 1,2"]
    self.assertEqual(cmd_actual, cmd_expected)

  def test_reorder_compose_sort(self):

    sort_by_cols = ['col_c','col_a']
    self.table_a.sort(sort_by_cols)

    col_order = ['col_a','col_b']
    self.table_a.order_columns(col_order)

    cmd_actual = self.table_a.cmd
    cmd_expected = ["cut -d, -f3,1,2,4", "sort -t, -k 1,2", "cut -d, -f2,3,1,4"]
    self.assertEqual(cmd_actual, cmd_expected)
    
