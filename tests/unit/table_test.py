import unittest
import os
from sqltxt.table import Table 
from sqltxt.column import Column, ColumnName, AmbiguousColumnNameError
from sqltxt.expression import Expression

class TableTest(unittest.TestCase):

    def setUp(self):

        self.data_path = os.path.join(os.path.dirname(__file__), '../data')

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

    def test_subset_rows(self):
    
        conditions = [
            [Expression('col_b', '==', '1'), 'or', Expression('col_a', '==', '2')]
        ]
        self.table_a.subset_rows(conditions)
       
        cmds_actual = self.table_a.cmds
        cmds_expected = [
            'echo -e "1,1\n2,3\n3,2"',
            "awk -F',' 'OFS=\",\" { if (($2 == 1 || $1 == 2)) { print $1,$2 } }'"]
        self.assertEqual(cmds_actual, cmds_expected)

    def test_order_columns(self):

        col_name_order = [ColumnName('col_b'), ColumnName('col_a')]
        self.table_a.order_columns(col_name_order)
      
        cmds_actual = self.table_a.cmds
        cmds_expected = ['echo -e "1,1\n2,3\n3,2"', "awk -F',' 'OFS=\",\" { print $2,$1 }'"]
        self.assertEqual(cmds_actual, cmds_expected)

    def test_sort(self):
    
        sort_by_col_names = [ColumnName('col_a'), ColumnName('col_b')]
        self.table_a.sort(sort_by_col_names)

        cmds_actual = self.table_a.cmds
        cmds_expected = ['echo -e "1,1\n2,3\n3,2"', "sort -t, -k 1,1 -k 2,2"]
        self.assertEqual(cmds_actual, cmds_expected)

        sort_by_cols = [self.table_a.get_column_for_name(cn) for cn in sort_by_col_names]
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

    def test_get_column_for_name_raises_on_ambiguity(self):

        table_from_cmd = Table.from_cmd(
            name = 'table_a', 
            cmd = 'echo -e ""',
            columns = ['col_a', 'col_a'])

        with self.assertRaisesRegexp(AmbiguousColumnNameError, 'Ambiguous column reference'):
            table_from_cmd.get_column_for_name(ColumnName('col_a'))

        table_from_cmd = Table.from_cmd(
            name = 'table_a', 
            cmd = 'echo -e ""',
            columns = ['ta.col_a', 'tb.col_a'])

        with self.assertRaisesRegexp(AmbiguousColumnNameError, 'Ambiguous column reference'):
            table_from_cmd.get_column_for_name(ColumnName('col_a'))

        first_column = Column('ta.col_a')
        first_column.add_name('col_alpha')
        second_column = Column('tb.col_a')
        table_from_cmd = Table.from_cmd(
            name = 'table_a', 
            cmd = 'echo -e ""',
            columns = [first_column, second_column])

        with self.assertRaisesRegexp(AmbiguousColumnNameError, 'Ambiguous column reference'):
            table_from_cmd.get_column_for_name(ColumnName('col_a'))

    def test_sample_rows(self):
        self.table_a.sample_rows(1)
        cmds_actual = self.table_a.cmds
        cmds_expected = ['echo -e "1,1\n2,3\n3,2"',
        """awk -v seed=$RANDOM -v n={0} '
            BEGIN {{ srand(seed) }}
            NR <= n {{ reservoir[NR] = $0 }}
            NR > n {{ M = int(rand() * NR) + 1; if (M <= n) {{ reservoir[M] = $0 }}}}
            END {{ for (key in reservoir) {{ print reservoir[key] }}}}'""".format(1)
        ]
        self.assertEqual(cmds_actual, cmds_expected)

    def test_get_cmd_str(self):

        table_from_file = Table.from_file_path(os.path.join(self.data_path, 'table_a.txt'))

        # output from a file-backed Table to STDOUT
        cmd_actual = table_from_file.get_cmd_str()
        cmd_expected = 'tail -n+2 {}/table_a.txt'.format(self.data_path)
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
