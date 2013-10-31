import unittest
from table import Table
from query import Query 
from column import Column
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

  def test_join_columns(self):

    # TODO: decide whether to call generate_table from constructor
    q = Query(
      from_clauses = [['table_a'],[['inner','join'],'table_b','on',['table_a.col_a', '=', 'table_b.col_a']]], 
      where_clauses = [], 
      column_names = ['col_a', 'col_b', 'col_z'])
    t = q.generate_table()
    header_actual = t.columns
    header_expected = ['col_a', 'col_b', 'col_z']

    self.assertEqual([str(col) for col in header_actual], header_expected)

  def test_join_columns_retains_column_ancestry(self):

    q = Query(
      from_clauses = [['table_a'],[['inner','join'],'table_b','on',['table_a.col_a', '=', 'table_b.col_a']]], 
      where_clauses = [], 
      column_names = ['col_a', 'col_b', 'col_z'])
    t = q.generate_table()
    header_actual = t.columns

    parent_col = Column('table_b.col_a')
    self.assertEqual([header_actual[0]], parent_col.search(header_actual,True))

  def test_join_two_tables(self):
    
    q = Query(
      from_clauses = [['table_a'],[['inner','join'],'table_b','on',['table_a.col_a', '=', 'table_b.col_a']]], 
      where_clauses = [], 
      column_names = ['col_a', 'col_b', 'col_z'])

    table_actual = q.generate_table()
    table_expected = Table.from_cmd(
      name = 'table_a', 
      cmd = 'echo -e "1,1,w\n2,3,x\n2,3,y"',
      columns = ['col_a','col_b','col_z']
      )

    table_expected_out = subprocess.check_output(['/bin/bash', '-c', table_expected.get_cmd_str(output_column_names=True)])
    table_actual_out = subprocess.check_output(['/bin/bash', '-c', table_actual.get_cmd_str(output_column_names=True)])
    
    self.assertEqual(table_actual_out, table_expected_out)

  def test_join_two_tables_with_sort(self):
    
    q = Query(
      from_clauses = [['table_a'],[['inner','join'],'table_b','on',['table_a.col_b', '=', 'table_b.col_a']]], 
      where_clauses = [], 
      column_names = ['col_b', 'col_a', 'col_z'])
    table_actual = q.generate_table()
    cmd_actual = table_actual.get_cmd_str(output_column_names=True)
    cmd_expected = \
      'echo "col_b,col_a,col_z"; ' + \
      "join -t, -1 2 -2 1 <(tail +2 TABLE_A.txt | sort -t, -k 2) <(tail +2 TABLE_B.txt | sort -t, -k 1)"
    self.assertEqual(cmd_actual, cmd_expected)
    
    table_actual_out = subprocess.check_output(['/bin/bash', '-c', cmd_actual])
    table_expected_out = subprocess.check_output(['/bin/bash', '-c', cmd_expected])

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
      "join -t, -1 2 -2 1 <(tail +2 TABLE_A.txt | sort -t, -k 2) <(tail +2 TABLE_B.txt | sort -t, -k 1)"
    self.assertEqual(cmd_actual, cmd_expected)
    
    table_actual_out = subprocess.check_output(['/bin/bash', '-c', cmd_actual])
    table_expected_out = subprocess.check_output(['/bin/bash', '-c', cmd_expected])

    self.assertEqual(table_actual_out, table_expected_out)
    
