import unittest
from table import Table
from query import Query 
import subprocess

class QueryTest(unittest.TestCase):

  def setUp(self):

    table_header = ["col_a", "col_b", "col_c", "col_d"]
    table_contents = """1,4,2,1
2,3,1,4
3,2,4,2
4,1,3,3"""

    self.table_a = Table(
      'table_a', 
      cmd = 'echo -e {0}'.format(table_contents), 
      column_names = table_header
      )

    table_header = ["col_a", "col_b"]
    table_contents = """1,w
2,x
3,y
4,z"""

    self.table_b = Table(
      'table_b', 
      cmd = 'echo -e {0}'.format(table_contents), 
      column_names = table_header
      )

  def test_qualify_column_names(self):
    
    unqualified_column_names = ['col_a', 'col_c', 'col_dne']
    names_expected = {
      'col_a': ['table_a.col_a', 'table_b.col_a'],
      'col_c': ['table_a.col_c'],
      'col_dne': []
      }
    names_actual = Query._qualify_column_names(unqualified_column_names, [self.table_a, self.table_b])

    self.assertEqual(names_actual, names_expected)

  def test_select(self):

    q = Query(['col_a', 'col_b'], [['table_a']], [])
    table_actual = q.generate_table()

    contents_expected = "1,4\n2,3\n3,2\n4,1"
    header_expected = ["col_a", "col_b"]
    table_expected = Table(
      'expected', 
      cmd = 'echo -e "{0}"'.format(contents_expected), 
      column_names = header_expected 
      )

    table_expected_out = subprocess.check_output(['/bin/bash', '-c', table_expected.get_cmd_str()])
    table_actual_out = subprocess.check_output(['/bin/bash', '-c', table_actual.get_cmd_str()])
    self.assertEqual(table_actual_out, table_expected_out)
          
  def test_where(self):

    q = Query(['col_a', 'col_b'], [['table_a']], [['col_c', '<', '3']])
    table_actual = q.generate_table()

    contents_expected = "1,4\n2,3"
    header_expected = ['col_a', 'col_b']
    table_expected = Table(
      'expected', 
      cmd = 'echo -e "{0}"'.format(contents_expected), 
      column_names = header_expected 
      )

    table_expected_out = subprocess.check_output(['/bin/bash', '-c', table_expected.get_cmd_str()])
    table_actual_out = subprocess.check_output(['/bin/bash', '-c', table_actual.get_cmd_str()])
    self.assertEqual(table_actual_out, table_expected_out)

  def test_join_two_tables(self):
    
    join_condition = ['table_a.col_a', '=', 'table_b.col_a']
    cmd_actual = q.join(table_a, table_b, join_condition)
    
    cmd_expected = "join -t, -1 1 -2 1 table_a.txt table_b.txt"
    

  def test_join_two_tables_with_sort(self):
    
    join_condition = ['table_a.col_a', '=', 'table_b.col_b']
    cmd_actual = Query.join(table_a, table_b, join_condition)
    
    cmd_expected = \
      "sort -t, -k 2 table_b.txt > table_b.txt.out; " + \
      "join -t, -1 1 -2 2 table_a.txt table_b.txt.out"
    
