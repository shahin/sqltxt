import unittest
from table import Table
from query import Query 
from column import Column
import subprocess

class QueryFunctionalTest(unittest.TestCase):

  def setUp(self):

    table_header = ["col_a", "col_b"]
    table_contents = """1,1\n2,3\n3,2"""

    self.table_a = Table.from_cmd(
      name = 'table_a', 
      cmd = 'echo -e "{0}"'.format(table_contents), 
      columns = table_header
      )

    table_header = ["col_a", "col_z"]
    table_contents = """1,w\n2,x\n2,y\n5,z"""

    self.table_b = Table.from_cmd(
      name = 'table_b', 
      cmd = 'echo -e "{0}"'.format(table_contents), 
      columns = table_header
      )

  def test_select(self):

    q = Query([['table_a']], [], ['col_b'])
    table_actual = q.generate_table()

    table_expected = Table.from_cmd(
      name = 'expected', 
      cmd = 'echo -e "1\n3\n2"',
      columns = ["col_b"] 
      )

    table_expected_out = subprocess.check_output(['/bin/bash', '-c', table_expected.get_cmd_str(output_column_names=True)])
    table_actual_out = subprocess.check_output(['/bin/bash', '-c', table_actual.get_cmd_str(output_column_names=True)])
    self.assertEqual(table_actual_out, table_expected_out)
          
  def test_where(self):

    q = Query([['table_a']], [['col_b', '<', '3']], ['col_a'])
    table_actual = q.generate_table()

    table_expected = Table.from_cmd(
      'expected', 
      cmd = 'echo -e "1\n3"',
      columns = ['col_a'] 
      )

    table_expected_out = subprocess.check_output(['/bin/bash', '-c', table_expected.get_cmd_str(output_column_names=True)])
    table_actual_out = subprocess.check_output(['/bin/bash', '-c', table_actual.get_cmd_str(output_column_names=True)])
    self.assertEqual(table_actual_out, table_expected_out)

  def test_wildcard_selects_all_columns(self):
    
    q = Query(
      from_clauses = [['table_a']], 
      where_clauses = [], 
      column_names = ['*'])
    table_actual = q.generate_table()
    cmd_actual = table_actual.get_cmd_str(output_column_names=True)
    cmd_expected = 'echo "col_a,col_b"; tail +2 TABLE_A.txt'
    self.assertEqual(cmd_actual, cmd_expected)
    
    table_actual_out = subprocess.check_output(['/bin/bash', '-c', cmd_actual])
    table_expected_out = subprocess.check_output(['/bin/bash', '-c', cmd_expected])

    self.assertEqual(table_actual_out, table_expected_out)

  def test_wildcard_on_join_selects_all_columns(self):
    
    q = Query(
      from_clauses = [['table_a'],[['inner','join'],'table_b','on',['table_a.col_b', '=', 'table_b.col_a']]], 
      where_clauses = [], 
      column_names = ['*'])
    table_actual = q.generate_table()
    cmd_actual = table_actual.get_cmd_str(output_column_names=True)
    cmd_expected = \
      'echo "col_b,col_a,col_z"; ' + \
      "join -t, -1 2 -2 1 <(tail +2 TABLE_A.txt | sort -t, -k 2,2) <(tail +2 TABLE_B.txt | sort -t, -k 1,1)"
    self.assertEqual(cmd_actual, cmd_expected)
    
    table_actual_out = subprocess.check_output(['/bin/bash', '-c', cmd_actual])
    table_expected_out = subprocess.check_output(['/bin/bash', '-c', cmd_expected])

    self.assertEqual(table_actual_out, table_expected_out)
    
