import unittest
from table import Table 
from table import compose_cmd_str
from column import Column

class TableFunctionalTest(unittest.TestCase):

  def test_join_two_tables(self):
    
    from table import join_tables
    table_actual = join_tables(self.table_a, self.table_b, [['table_a.col_a', '=', 'table_b.col_a']])
    table_expected = Table.from_cmd(
      name = 'table_a', 
      cmd = 'echo -e "1,1,w\n2,3,x\n2,3,y"',
      columns = ['col_a','col_b','col_z']
      )

    table_expected_out = subprocess.check_output(['/bin/bash', '-c', table_expected.get_cmd_str(output_column_names=True)])
    table_actual_out = subprocess.check_output(['/bin/bash', '-c', table_actual.get_cmd_str(output_column_names=True)])
    
    self.assertEqual(table_actual_out, table_expected_out)

  @unittest.skip
  def test_join_two_tables_with_sort(self):
    
    q = Query(
      from_clauses = [['table_a'],[['inner','join'],'table_b','on',['table_a.col_b', '=', 'table_b.col_a']]], 
      where_clauses = [], 
      column_names = ['col_b', 'col_a', 'col_z'])
    table_actual = q.generate_table()
    cmd_actual = table_actual.get_cmd_str(output_column_names=True)
    cmd_expected = \
      'echo "col_b,col_a,col_z"; ' + \
      "join -t, -1 2 -2 1 <(tail +2 TABLE_A.txt | sort -t, -k 2,2) <(tail +2 TABLE_B.txt | sort -t, -k 1,1)"
    self.assertEqual(cmd_actual, cmd_expected)
    
    table_actual_out = subprocess.check_output(['/bin/bash', '-c', cmd_actual])
    table_expected_out = subprocess.check_output(['/bin/bash', '-c', cmd_expected])

    self.assertEqual(table_actual_out, table_expected_out)


