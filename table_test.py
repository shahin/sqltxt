import unittest
from table import Table 
from column import Column

class TableTest(unittest.TestCase):

  def setUp(self):

    table_header = ["col_a", "col_b"]
    table_contents = """1,1
2,3
3,2"""

    self.table_a = Table.from_cmd(
      name = 'table_a', 
      cmd = 'echo -e "{0}"'.format(table_contents), 
      columns = table_header
      )

    table_header = ["col_a", "col_b"]
    table_contents = """1,w
2,x
2,y
5,z"""

    self.table_b = Table.from_cmd(
      name = 'table_b', 
      cmd = 'echo -e "{0}"'.format(table_contents), 
      columns = table_header
      )

  def test_qualify_column(self):

    result_actual = self.table_b._qualify_column(Column('col_a'))
    result_expected = Column('table_b.col_a')
    self.assertEqual(result_actual, result_expected)

    result_actual = self.table_b._qualify_column(Column('table_b.col_a'))
    result_expected = Column('table_b.col_a')
    self.assertEqual(result_actual, result_expected)

    result_actual = self.table_b._qualify_column(Column('table_a.col_a'))
    result_expected = Column('table_a.col_a')
    self.assertNotEqual(result_actual, result_expected)

  def test_subset_columns(self):
    
    conditions = [['col_b', '==', '1'], 'or', ['col_a', '==', '2']]
    self.table_a.subset_columns(conditions)

    cmds_actual = self.table_a.cmds
    cmds_expected = [
        'echo -e "1,1\n2,3\n3,2"',
        "awk -F',' 'OFS=\",\" { if ($2 == 1 || $1 == 2) { print $1,$2 } }'"]
    self.assertEqual(cmds_actual, cmds_expected)

  def test_reorder(self):

    col_order = [Column('col_b'), Column('col_a')]
    self.table_a.order_columns(col_order)

    cmds_actual = self.table_a.cmds
    cmds_expected = ['echo -e "1,1\n2,3\n3,2"', "awk -F',' 'OFS=\",\" { print $2,$1 }'"]
    self.assertEqual(cmds_actual, cmds_expected)

  def test_sort(self):
    
    sort_by_cols = [Column('col_a'), Column('col_b')]
    self.table_a.sort(sort_by_cols)

    cmds_actual = self.table_a.cmds
    cmds_expected = ['echo -e "1,1\n2,3\n3,2"', "sort -t, -k 1,1 -k 2,2"]
    self.assertEqual(cmds_actual, cmds_expected)

    self.assertEqual(self.table_a.sorted_by, sort_by_cols)

  def test_is_sorted_by(self):

    table_from_cmd = Table.from_cmd(
      name = 'table_a', 
      cmd = 'echo -e ""',
      columns = ['col_a', 'col_b'])

    table_from_cmd.sorted_by = [Column('table_a.col_a'), Column('table_a.col_b')]

    self.assertTrue(table_from_cmd.is_sorted_by([0]))
    self.assertFalse(table_from_cmd.is_sorted_by([1]))
    self.assertTrue(table_from_cmd.is_sorted_by([0,1]))

  def test_count_star(self):

    # select col_a, count(col_b) from table_b group by col_a
    self.table_b.group_by(['col_a','col_b'], 'count(*)')
    cmds_actual = self.table_b.cmds

    # group by col_a, col_b should have a row for every value of col_a x col_b, even if that value
    # is NULL
    # this will not include rows for col_a values that have zero non-null col_b's
    # not sure how to do that yet
    cmds_expected = [
      'echo -e "1,x\n2,w\n2,y\n3,z"', 
      'tee <(',
      [
        'cut -d, -f1',
        'sort -t, -k 1.1',
        'uniq -c',
        '> count_col_b_group_by_col_a_fifo'
      ],
      ')',
      'cut -d, -f1',
      'sort -t, -k 1.1',
      '> group_by_col_a_fifo',
      'join  <(group_by_col_a_fifo) <(count_col_b_group_by_col_a_fifo)'
    ]

    self.assertEqual(cmds_actual, cmds_expected)

  def test_count_var(self):

    # select col_a, count(col_b) from table_b group by col_a
    self.table_b.group_by(['col_a'], 'count(col_b)')
    cmds_actual = self.table_b.cmds

    # group by col_a, col_b should have a row for every value of col_a x col_b, even if that value
    # is NULL
    # this will not include rows for col_a values that have zero non-null col_b's
    # not sure how to do that yet
    cmds_expected = [
      'echo -e "1,x\n2,w\n2,y\n3,z"', 
      'tee <(',
      [
        < 'awk \'BEGIN { FS = "," }; { if($2 != "") print }\'' >
        'cut -d, -f1',
        'sort -t, -k 1.1',
        'uniq -c',
        '> count_col_b_group_by_col_a_fifo'
      ],
      ')',
      'cut -d, -f1',
      'sort -t, -k 1.1',
      '> group_by_col_a_fifo',
      'join  <(group_by_col_a_fifo) <(count_col_b_group_by_col_a_fifo)'
    ]

    self.assertEqual(cmds_actual, cmds_expected)


  def test_get_cmd_str(self):
    
    table_from_file = Table.from_filename('table_a')

    # output from a file-backed Table to STDOUT
    cmd_actual = table_from_file.get_cmd_str()
    cmd_expected = 'tail +2 table_a.txt'
    self.assertEqual(cmd_actual, cmd_expected)

    table_from_cmd = Table.from_cmd(
      'table_a', 
      cmd = 'echo -e "1,2,3,4"', 
      columns = ['col_a', 'col_b', 'col_c', 'col_d'])

    # output from a command-backed Table to STDOUT
    cmd_actual = table_from_cmd.get_cmd_str()
    cmd_expected = 'echo -e "1,2,3,4"'
    self.assertEqual(cmd_actual, cmd_expected)
    
    # add a command, then output
    table_from_cmd.cmds += ['sort']

    # to STDOUT
    cmd_actual = table_from_cmd.get_cmd_str()
    cmd_expected = 'echo -e "1,2,3,4" | sort'
    self.assertEqual(cmd_actual, cmd_expected)
