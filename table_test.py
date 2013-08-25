import unittest
from table import Table 

class TableTest(unittest.TestCase):

  def setUp(self):
    self.table_a = Table('table_a')

  def test_select_subset(self):
    
    conditions = [['col_b', '==', '2'], 'or', ['col_a', '==', '4']]
    self.table_a.select_subset(conditions)

    cmds_actual = self.table_a.cmds
    cmds_expected = ["awk -F',' 'OFS=\",\" { if ($2 == 2 || $1 == 4) { print $1,$2,$3,$4 } }'"]
    self.assertEqual(cmds_actual, cmds_expected)

  def test_reorder(self):

    col_order = ['col_c','col_a']
    self.table_a.order_columns(col_order)

    cmds_actual = self.table_a.cmds
    cmds_expected = ["cut -d, -f3,1,2,4"]
    self.assertEqual(cmds_actual, cmds_expected)

  def test_sort(self):
    
    sort_by_cols = ['col_a','col_b']
    self.table_a.sort(sort_by_cols)

    cmds_actual = self.table_a.cmds
    cmds_expected = ["sort -t, -k 1,2"]
    self.assertEqual(cmds_actual, cmds_expected)

    self.assertEqual(self.table_a.sorted_by, sort_by_cols)

  def test_sort_with_reordered_columns(self):
    
    sort_by_cols = ['col_c','col_a']
    self.table_a.sort(sort_by_cols)

    cmds_actual = self.table_a.cmds
    cmds_expected = ["cut -d, -f3,1,2,4", "sort -t, -k 1,2"]
    self.assertEqual(cmds_actual, cmds_expected)

  def test_reorder_compose_sort(self):

    sort_by_cols = ['col_c','col_a']
    self.table_a.sort(sort_by_cols)

    col_order = ['col_a','col_b']
    self.table_a.order_columns(col_order)

    cmds_actual = self.table_a.cmds
    cmds_expected = ["cut -d, -f3,1,2,4", "sort -t, -k 1,2", "cut -d, -f2,3,1,4"]
    self.assertEqual(cmds_actual, cmds_expected)

  def test_get_cmd_str(self):
    
    table_from_file = Table('table_a')

    # output from a file-backed Table to STDOUT
    cmd_actual = table_from_file.get_cmd_str()
    cmd_expected = 'tail +2 table_a.txt'
    self.assertEqual(cmd_actual, cmd_expected)

    table_from_cmd = Table(
      'table_a', 
      cmd = 'echo -e "1,2,3,4"', 
      column_names = ['col_a', 'col_b', 'col_c', 'col_d'])

    # output from a command-backed Table to STDOUT
    cmd_actual = table_from_cmd.get_cmd_str()
    cmd_expected = 'echo -e "1,2,3,4"'
    self.assertEqual(cmd_actual, cmd_expected)
    
    # output from a command-backed Table to a file
    cmd_actual = table_from_cmd.get_cmd_str('out_file.out')
    cmd_expected = 'echo -e "1,2,3,4" > out_file.out'
    self.assertEqual(cmd_actual, cmd_expected)
    
    # add a command, then output
    table_from_cmd.cmds += ['sort']

    # to STDOUT
    cmd_actual = table_from_cmd.get_cmd_str()
    cmd_expected = 'echo -e "1,2,3,4" | sort'
    self.assertEqual(cmd_actual, cmd_expected)
    
    # to a file
    cmd_actual = table_from_cmd.get_cmd_str('out_file.out')
    cmd_expected = 'echo -e "1,2,3,4" | sort > out_file.out'
    self.assertEqual(cmd_actual, cmd_expected)

  def test_get_pipe_name(self):
    
    test_table = Table('table_a')
    name_expected = 'table_a.fifo'
    name_actual = test_table.get_pipe_name()
    self.assertEqual(name_actual, name_expected)

  def test_get_cmd_to_pipe_str(self):

    # output to a named pipe
    table_from_file = Table('table_a')
    cmd_actual = table_from_file.get_cmd_to_pipe_str()
    cmd_expected = 'mkfifo table_a.fifo; tail +2 table_a.txt > table_a.fifo'
    self.assertEqual(cmd_actual, cmd_expected)

