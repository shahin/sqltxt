import unittest
from query import Query 

class QueryTest(unittest.TestCase):

  def setUp(self):

    table_header = "col_a,col_b,col_c,col_d"
    table_contents = """1,4,2,1
2,3,1,4
3,2,4,2
4,1,3,3"""

    table_a = Table(
      'table_a', 
      cmd = 'echo -e {0}'.format(self.table_contents), 
      column_names = self.header
      )

  def test_select(self):

    q = Query('select col_a, col_b from table_a')

    contents_expected = "1,4\n2,3\n3,2\n4,1"
    header_expected = "col_a,col_b"
    table_expected = Table(
      'expected', 
      cmd = 'echo -e {0}'.format(contents_expected), 
      column_names = header_expected 
      )

    table_actual = q.execute()
          
  def test_where(self):

    q = Query('select col_a, col_b from table_a where col_c < 3')

    contents_expected = "1,4\n2,3"
    header_expected = "col_a,col_b"
    table_expected = Table(
      'expected', 
      cmd = 'echo -e {0}'.format(contents_expected), 
      column_names = header_expected 
      )

    table_actual = q.execute()

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
    
