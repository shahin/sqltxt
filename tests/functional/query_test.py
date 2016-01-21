import unittest
import os
from sqltxt.table import Table
from sqltxt.query import Query, condition_applies, assign_columns, assign_conditions
from sqltxt.column import Column, ColumnName, AmbiguousColumnNameError
from sqltxt.expression import Expression, And, Or
import subprocess

class QueryTest(unittest.TestCase):

    def setUp(self):

        # TODO: replace this hack to make sure test files are found with fixtures
        if 'tests/data' not in os.getcwd():
          os.chdir(os.path.join(os.getcwd(), 'tests/data'))

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

    def test_condition_applies(self):

        condition = AndList([
            Expression('table_a.col_a', '==', 'table_b.col_z'),
            Expression('table_a.col_b', '==', 'table_b.col_a')
        ])
        self.assertTrue(condition_applies(condition, self.table_a, self.table_b))

        condition = OrList([
            Expression('table_a.col_a', '==', 'table_b.col_z'),
            Expression('table_a.col_b', '==', 'table_b.col_a')
        ])
        self.assertTrue(condition_applies(condition, self.table_a, self.table_b))

        condition = OrList([
            Expression('table_a.col_a', '==', 'table_b.col_z'),
            Expression('table_a.col_b', '==', 'table_b.col_a')
        ])
        self.assertFalse(condition_applies(condition, self.table_a))

        condition = AndList([
            Expression('table_a.col_a', '==', 'table_b.col_z'),
            Expression('table_c.col_b', '==', 'table_b.col_a')
        ])
        self.assertFalse(condition_applies(condition, self.table_a, self.table_b))

        condition = OrList([
            Expression('table_a.col_a', '==', 'table_b.col_j'),
            Expression('table_a.col_b', '==', 'table_b.col_a')
        ])
        self.assertFalse(condition_applies(condition, self.table_a, self.table_b))

        condition = AndList([
            Expression('table_a.col_a', '==', '1'),
            Expression('table_a.col_b', '==', 'table_b.col_a')
        ])
        self.assertTrue(condition_applies(condition, self.table_a, self.table_b))

        with self.assertRaises(AmbiguousColumnNameError):
            condition = AndList([
                Expression('table_a.col_a', '==', '1'),
                Expression('table_a.col_b', '==', 'col_a')
            ])
            condition_applies(condition, self.table_a, self.table_b)

    def test_assign_columns(self):

        column_names = [
            ColumnName('table_a.col_a'),
            ColumnName('table_b.col_a'),
            ColumnName('col_z'),
            ColumnName('col_b'),
        ]
        expected_assignment = [
            [ColumnName('table_a.col_a'), ColumnName('col_b'), ],
            [ColumnName('table_b.col_a'), ColumnName('col_z'), ]
        ]
        actual_assignment = assign_columns([self.table_a, self.table_b], column_names)
        self.assertEqual(expected_assignment, actual_assignment)

        with self.assertRaises(AmbiguousColumnNameError):
            column_names = [ColumnName('col_a'), ]
            actual_assignment = assign_columns([self.table_a, self.table_b], column_names)

    def test_assign_conditions(self):

        conditions = [
            Expression('table_a.col_a', '==', 'table_b.col_z'),
            Expression('table_a.col_a', '==', 'table_a.col_b'),
            OrList([
                Expression('table_a.col_a', '==', 'table_b.col_z'),
                Expression('table_a.col_a', '==', 'table_a.col_b')
            ]),
        ]

        expected_condition_order = [
            [Expression('table_a.col_a', '==', 'table_a.col_b')],
            [
                Expression('table_a.col_a', '==', 'table_b.col_z'),
                OrList([
                    Expression('table_a.col_a', '==', 'table_b.col_z'),
                    Expression('table_a.col_a', '==', 'table_a.col_b')
                ])
            ],
        ]
        actual_condition_order = assign_conditions([self.table_a, self.table_b], conditions)
        self.assertEqual(expected_condition_order, actual_condition_order)


    def test_select(self):

        query = Query({'left_relation': {'path': 'table_a.txt'}}, [], ['col_b'])
        table_actual = query.generate_table()

        table_expected = Table.from_cmd(
            name = 'expected', 
            cmd = 'echo -e "1\n3\n2"',
            columns = ["col_b"] 
            )

        table_expected_out = subprocess.check_output(['/bin/bash', '-c', table_expected.get_cmd_str(output_column_names=True)])
        table_actual_out = subprocess.check_output(['/bin/bash', '-c', table_actual.get_cmd_str(output_column_names=True)])
        self.assertEqual(table_actual_out, table_expected_out)
          
    def test_where(self):

        query = Query(
            [{'path': 'table_a.txt', 'alias': 'table_a.txt'}],
            [['col_b', '<', '3']],
            ['col_a']
        )
        table_actual = query.generate_table()

        table_expected = Table.from_cmd(
          'expected', 
          cmd = 'echo -e "1\n3"',
          columns = ['col_a'] 
          )

        table_expected_out = subprocess.check_output(['/bin/bash', '-c', table_expected.get_cmd_str(output_column_names=True)])
        table_actual_out = subprocess.check_output(['/bin/bash', '-c', table_actual.get_cmd_str(output_column_names=True)])
        self.assertEqual(table_actual_out, table_expected_out)

    def test_join_columns(self):

        query = Query(
          from_clause = {
              'left_relation': {'path': 'table_a.txt'},
              'joins': [[{
                  'join_type': 'inner',
                  'right_relation': {'path': 'table_b.txt'},
                  'join_conditions': [{
                      'left_operand': 'table_a.txt.col_a',
                      'right_operand': 'table_b.txt.col_a',
                      'operator': '='
                      }]
              }]]
          },
          where_clauses = [], 
          columns = ['col_a', 'col_b', 'col_z'])
        t = query.generate_table()
        header_actual = t.columns
        header_expected = ['col_a', 'col_b', 'col_z']

        self.assertEqual([str(col) for col in header_actual], header_expected)

    def test_join_two_tables(self):
        
        query = Query(
          from_clause = {
              'left_relation': {'path': 'table_a.txt'},
              'joins': [[{
                  'join_type': 'inner',
                  'right_relation': {'path': 'table_b.txt'},
                  'join_conditions': [{
                      'left_operand': 'table_a.txt.col_a',
                      'right_operand': 'table_b.txt.col_a',
                      'operator': '='
                  }]
              }]]
          },
          where_clauses = [],
          columns = ['col_a', 'col_b', 'col_z'])

        table_actual = query.generate_table()
        table_expected = Table.from_cmd(
          name = 'table_a', 
          cmd = 'echo -e "1,1,w\n2,3,x\n2,3,y"',
          columns = ['col_a','col_b','col_z']
          )

        table_expected_out = subprocess.check_output(['/bin/bash', '-c', table_expected.get_cmd_str(output_column_names=True)])
        table_actual_out = subprocess.check_output(['/bin/bash', '-c', table_actual.get_cmd_str(output_column_names=True)])
        
        self.assertEqual(table_actual_out, table_expected_out)

    def test_join_two_tables_with_sort(self):
        
        query = Query(
          from_clause = {
              'left_relation': {'path': 'table_a.txt'},
              'joins': [[{
                  'join_type': 'inner',
                  'right_relation': {'path': 'table_b.txt'},
                  'join_conditions': [{
                      'left_operand': 'table_a.txt.col_b',
                      'right_operand': 'table_b.txt.col_a',
                      'operator': '=',
                  }]
              }]]
          },
          where_clauses = [], 
          columns = ['col_b', 'table_b.txt.col_a', 'col_z'])
        table_actual = query.generate_table()
        cmd_actual = table_actual.get_cmd_str(output_column_names=True)
        cmd_expected = \
          'echo "col_b,col_a,col_z"; ' + \
          "join -t, -1 2 -2 1 <(tail +2 table_a.txt | sort -t, -k 2,2) <(tail +2 table_b.txt | sort -t, -k 1,1) | awk -F\',\' \'OFS=\",\" { print $1,$1,$3 }\'"
        assert cmd_actual == cmd_expected
        
        table_actual_out = subprocess.check_output(['/bin/bash', '-c', cmd_actual])
        table_expected_out = subprocess.check_output(['/bin/bash', '-c', cmd_expected])

        self.assertEqual(table_actual_out, table_expected_out)

    @unittest.skip('wildcards not supported')
    def test_wildcard_selects_all_columns(self):
        
        query = Query(
            from_clause = {'left_relation': {'path': 'table_a.txt'}}, 
            where_clauses = [], 
            columns = ['*'])
        table_actual = query.generate_table()
        cmd_actual = table_actual.get_cmd_str(output_column_names=True)
        cmd_expected = 'echo "col_a,col_b"; tail +2 TABLE_A.txt'
        self.assertEqual(cmd_actual, cmd_expected)
        
        table_actual_out = subprocess.check_output(['/bin/bash', '-c', cmd_actual])
        table_expected_out = subprocess.check_output(['/bin/bash', '-c', cmd_expected])

        self.assertEqual(table_actual_out, table_expected_out)

    @unittest.skip('wildcards not supported')
    def test_wildcard_on_join_selects_all_columns(self):
        
        query = Query(
          from_clause = [['table_a'],[['inner','join'],'table_b','on',['table_a.col_b', '=', 'table_b.col_a']]], 
          where_clauses = [], 
          columns = ['*'])
        table_actual = query.generate_table()
        cmd_actual = table_actual.get_cmd_str(output_column_names=True)
        cmd_expected = \
          'echo "col_b,col_a,col_z"; ' + \
          "join -t, -1 2 -2 1 <(tail +2 TABLE_A.txt | sort -t, -k 2,2) <(tail +2 TABLE_B.txt | sort -t, -k 1,1)"
        self.assertEqual(cmd_actual, cmd_expected)
        
        table_actual_out = subprocess.check_output(['/bin/bash', '-c', cmd_actual])
        table_expected_out = subprocess.check_output(['/bin/bash', '-c', cmd_expected])

        self.assertEqual(table_actual_out, table_expected_out)
        
