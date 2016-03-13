import unittest
import os
import subprocess
import re
import warnings

def get_awk_version():
    awk_version = None
    awk_version_str = subprocess.check_output(['awk -Wversion 2>/dev/null || awk --version'], shell=True)
    if re.compile('awk version').match(awk_version_str):
        awk_version = 'AWK'
    elif re.compile('GNU Awk').match(awk_version_str):
        awk_version = 'GAWK'
    else:
        warnings.warn("Unrecognized awk version: {}".format(awk_version))
    return awk_version

class SqltxtTest(unittest.TestCase):
        
    def test_select(self):
        cmd = "sqltxt 'select col_a from tests/data/table_a.txt'"
        actual_output = subprocess.check_output(['/bin/bash', '-c', cmd])
        expected_output = """echo "col_a"; tail -n+2 tests/data/table_a.txt | awk -F',' 'OFS="," { print $1 }'\n"""
        self.assertEqual(expected_output, actual_output)

    def test_executed_select(self):
        cmd = "sqltxt -e 'select col_a from tests/data/table_a.txt'"
        actual_output = subprocess.check_output(['/bin/bash', '-c', cmd])
        expected_output = """col_a\n1\n2\n3\n\n"""
        self.assertEqual(expected_output, actual_output)

    def test_where(self):
        cmd = "sqltxt 'select col_a from tests/data/table_a.txt where col_b > 2'"
        actual_output = subprocess.check_output(['/bin/bash', '-c', cmd])
        expected_output = """echo "col_a"; tail -n+2 tests/data/table_a.txt | awk -F',' 'OFS="," { if ($2 > 2) { print $1,$2 } }' | awk -F',' 'OFS="," { print $1 }'\n"""
        self.assertEqual(expected_output, actual_output)

    def test_executed_where(self):
        cmd = "sqltxt -e 'select col_a from tests/data/table_a.txt where col_b > 2'"
        actual_output = subprocess.check_output(['/bin/bash', '-c', cmd])
        expected_output = """col_a\n2\n\n"""
        self.assertEqual(expected_output, actual_output)

    def test_executed_join(self):
        cmd = "sqltxt -e 'select ta.col_a, col_z from tests/data/table_a.txt ta join tests/data/table_b.txt tb on (ta.col_a = tb.col_a) where col_b > 1'"
        actual_output = subprocess.check_output(['/bin/bash', '-c', cmd])
        expected_output = """col_a,col_z\n2,x\n2,y\n\n"""
        self.assertEqual(expected_output, actual_output)

    def test_rows_are_sampled(self):

        cmd = "sqltxt -e --random-seed=100 'select ta.col_a, col_z from tests/data/table_a.txt ta join tests/data/table_b.txt tb on (ta.col_a = tb.col_a) tablesample (1)'"
        actual_output = subprocess.check_output(['/bin/bash', '-c', cmd])
        expected_output_for_awk = {
            'GAWK': """col_a,col_z\n2,x\n\n""",
            'AWK': """col_a,col_z\n2,y\n\n""",
        }
        awk_version = get_awk_version() or 'AWK'
        expected_output = expected_output_for_awk[awk_version]
        self.assertEqual(expected_output, actual_output)

        cmd = "sqltxt -e --random-seed=101 'select ta.col_a, col_z from tests/data/table_a.txt ta join tests/data/table_b.txt tb on (ta.col_a = tb.col_a) tablesample (1)'"
        actual_output = subprocess.check_output(['/bin/bash', '-c', cmd])
        expected_output = """col_a,col_z\n1,w\n\n"""
        self.assertEqual(expected_output, actual_output)

        cmd = "sqltxt -e --random-seed=101 'select ta.col_a, col_z from tests/data/table_a.txt ta join tests/data/table_b.txt tb on (ta.col_a = tb.col_a) tablesample (2)'"
        actual_output = subprocess.check_output(['/bin/bash', '-c', cmd])
        expected_output = """col_a,col_z\n2,x\n1,w\n\n"""
        self.assertEqual(expected_output, actual_output)
