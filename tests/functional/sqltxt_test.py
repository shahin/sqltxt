import unittest
import os
import subprocess

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
        expected_output = """echo "col_a"; tail -n+2 tests/data/table_a.txt | awk -F',' 'OFS="," { if ($2 > 1) { print $1,$2 } }' | awk -F',' 'OFS="," { print $1 }'\n"""
        expected_output = """col_a,col_z\n2,x\n2,y\n\n"""
        self.assertEqual(expected_output, actual_output)
