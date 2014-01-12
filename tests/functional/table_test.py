import unittest
from table import Table 
from table import compose_cmd_str
from column import Column
import subprocess

class TableFunctionalTest(unittest.TestCase):

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

    table_header = ["col_a", "col_z"]
    table_contents = """1,w
2,x
2,y
5,z"""

    self.table_b = Table.from_cmd(
      name = 'table_b', 
      cmd = 'echo -e "{0}"'.format(table_contents), 
      columns = table_header
      )


  def test_join_two_tables(self):
    
    from table import join_tables
    table_actual = join_tables(self.table_a, self.table_b, [['table_a.col_a', '=', 'table_b.col_a']])
    table_actual_out = subprocess.check_output(['/bin/bash', '-c', table_actual.get_cmd_str(output_column_names=True)])
    table_expected_out = b'col_a,col_b,col_z\n1,1,w\n2,3,x\n2,3,y\n'
    
    self.assertEqual(table_actual_out, table_expected_out)

  def test_join_two_tables_with_sort(self):
    
    from table import join_tables
    table_actual = join_tables(self.table_a, self.table_b, [['table_a.col_b', '=', 'table_b.col_a']])
    cmd_actual = table_actual.get_cmd_str(output_column_names=True)
    table_actual_out = subprocess.check_output(['/bin/bash', '-c', cmd_actual])
    table_expected_out = b'col_b,col_a,col_z\n1,1,w\n2,3,x\n2,3,y\n'

    self.assertEqual(table_actual_out, table_expected_out)

