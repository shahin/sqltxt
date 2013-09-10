import unittest
from table import Table 

class TableTest(unittest.TestCase):

  def setUp(self):

    table_header = ["col_a", "col_b"]
    table_contents = """1,1
2,3
3,2"""

    self.table_a = Table.from_cmd(
      name = 'table_a', 
      cmd = 'echo -e "{0}"'.format(table_contents), 
      column_names = table_header
      )

    table_header = ["col_a", "col_b"]
    table_contents = """1,w
2,x
2,y
5,z"""

    self.table_b = Table.from_cmd(
      name = 'table_b', 
      cmd = 'echo -e "{0}"'.format(table_contents), 
      column_names = table_header
      )

  def test_select_subset(self):
    
    conditions = [['col_b', '==', '1'], 'or', ['col_a', '==', '2']]
    self.table_a.select_subset(conditions)

    cmds_actual = self.table_a.cmds
    cmds_expected = [
        'echo -e "1,1\n2,3\n3,2"',
        "awk -F',' 'OFS=\",\" { if ($2 == 1 || $1 == 2) { print $1,$2 } }'"]
    self.assertEqual(cmds_actual, cmds_expected)

  def test_reorder(self):

    col_order = ['col_b','col_a']
    self.table_a.order_columns(col_order)

    cmds_actual = self.table_a.cmds
    cmds_expected = ['echo -e "1,1\n2,3\n3,2"', "awk -F',' 'OFS=\",\" { print $2,$1 }'"]
    self.assertEqual(cmds_actual, cmds_expected)

  def test_sort(self):
    
    sort_by_cols = ['col_a','col_b']
    self.table_a.sort(sort_by_cols)

    cmds_actual = self.table_a.cmds
    cmds_expected = ['echo -e "1,1\n2,3\n3,2"', "sort -t, -k 1,2"]
    self.assertEqual(cmds_actual, cmds_expected)

    self.assertEqual(self.table_a.sorted_by, sort_by_cols)

  def test_sort_with_reordered_columns(self):
    
    sort_by_cols = ['col_b','col_a']
    self.table_a.sort(sort_by_cols)

    cmds_actual = self.table_a.cmds
    cmds_expected = ['echo -e "1,1\n2,3\n3,2"', 
        "awk -F',' 'OFS=\",\" { print $2,$1 }'",
        "sort -t, -k 1,2"]
    self.assertEqual(cmds_actual, cmds_expected)

  def test_reorder_compose_sort(self):

    sort_by_cols = ['col_b','col_a']
    self.table_a.sort(sort_by_cols)

    col_order = ['col_a','col_b']
    self.table_a.order_columns(col_order)

    cmds_actual = self.table_a.cmds
    cmds_expected = ['echo -e "1,1\n2,3\n3,2"', 
        "awk -F',' 'OFS=\",\" { print $2,$1 }'",
        "sort -t, -k 1,2",
        "awk -F',' 'OFS=\",\" { print $2,$1 }'"]
    self.assertEqual(cmds_actual, cmds_expected)

  def test_is_sorted_by(self):

    table_from_cmd = Table.from_cmd(
      name = 'table_a', 
      cmd = 'echo -e ""',
      column_names = ['col_a', 'col_b'])

    table_from_cmd.sorted_by = ['col_a', 'col_b']

    self.assertTrue(table_from_cmd.is_sorted_by([0]))
    self.assertFalse(table_from_cmd.is_sorted_by([1]))
    self.assertTrue(table_from_cmd.is_sorted_by([0,1]))

  def test_get_cmd_str(self):
    
    table_from_file = Table.from_filename('table_a')

    # output from a file-backed Table to STDOUT
    cmd_actual = table_from_file.get_cmd_str()
    cmd_expected = 'tail +2 table_a.txt'
    self.assertEqual(cmd_actual, cmd_expected)

    table_from_cmd = Table.from_cmd(
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
