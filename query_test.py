import unittest
from table import Table
from query import Query 
import subprocess

class QueryTest(unittest.TestCase):

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

  def test_qualify_column_names(self):
    
    unqualified_column_names = ['col_a', 'col_dne']
    names_expected = {
      'col_a': ['table_a.col_a', 'table_b.col_a'],
      'col_dne': []
      }
    names_actual = Query._qualify_column_names(unqualified_column_names, [self.table_a, self.table_b])

    self.assertEqual(names_actual, names_expected)

  def test_select(self):

    q = Query([['table_a']], [], ['col_b'])
    table_actual = q.generate_table()

    contents_expected = "1\n3\n2"
    header_expected = ["col_b"]
    table_expected = Table.from_cmd(
      name = 'expected', 
      cmd = 'echo -e "{0}"'.format(contents_expected), 
      column_names = header_expected 
      )

    table_expected_out = subprocess.check_output(['/bin/bash', '-c', table_expected.get_cmd_str(output_column_names=True)])
    table_actual_out = subprocess.check_output(['/bin/bash', '-c', table_actual.get_cmd_str(output_column_names=True)])
    self.assertEqual(table_actual_out, table_expected_out)
          
  def test_where(self):

    q = Query([['table_a']], [['col_b', '<', '3']], ['col_a'])
    table_actual = q.generate_table()

    contents_expected = "1\n3"
    header_expected = ['col_a']
    table_expected = Table.from_cmd(
      'expected', 
      cmd = 'echo -e "{0}"'.format(contents_expected), 
      column_names = header_expected 
      )

    table_expected_out = subprocess.check_output(['/bin/bash', '-c', table_expected.get_cmd_str(output_column_names=True)])
    table_actual_out = subprocess.check_output(['/bin/bash', '-c', table_actual.get_cmd_str(output_column_names=True)])
    self.assertEqual(table_actual_out, table_expected_out)

  def test_join_two_tables(self):
    
    q = Query(['col_a', 'col_b'], [['table_a']], [])

    table_a = Table.from_filename('table_a')
    table_b = Table.from_filename('table_b')
    join_condition = [['table_a.col_a', '=', 'table_b.col_a']]
    result_table_actual = q.join(table_a, table_b, join_condition)
    cmd_actual = result_table_actual.get_cmd_str(output_column_names = True)
    
    cmd_expected = "join -t, -1 1 -2 1 table_a.txt table_b.txt"
    self.assertEqual(cmd_actual, cmd_expected)
    

  def test_join_two_tables_with_sort(self):
    
    q = Query(['col_a', 'col_b'], [['table_a'],['table_b']], [])
    join_condition = [['table_a.col_b', '=', 'table_b.col_a']]
    table_actual = q.join(self.table_a, self.table_b, join_condition)
    cmd_actual = table_actual.get_cmd_str(output_column_names=True)
    cmd_expected = \
      "join -t, -1 2 -2 1 <(tail +2 table_a.txt | sort -t, -k 2) <(tail +2 table_b.txt)"
    print(cmd_expected)
    
    table_actual_out = subprocess.check_output(['/bin/bash', '-c', cmd_actual])
    table_expected_out = subprocess.check_output(['/bin/bash', '-c', cmd_expected])

    self.assertEqual(table_actual_out, table_expected_out)
    
