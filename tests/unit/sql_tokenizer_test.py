import unittest
from sql_tokenizer import SqlTokenizer

class SqlTokenizerUnitTest(unittest.TestCase):

  def setUp(self):
    self.t = SqlTokenizer()

  def test_ordinary_column_names_are_tokenized(self):
    tokens = self.t.parse("select col_a, col_b, col_c from table_a")
    tokens.column_definitions.asList() == ['col_a','col_b','col_c']

  def test_wildcard_is_tokenized(self):
    tokens = self.t.parse("select * from table_a")
    tokens.column_definitions.asList() == ['*']
    tokens = self.t.parse("select col_a, *, col_b from table_a")
    tokens.column_definitions.asList() == ['col_a', '*', 'col_b']

  def test_aggregate_function_is_tokenized(self):
    tokens = self.t.parse("select count(*) from table_a")
    tokens.column_definitions.aggregate_functions.asList() == ['count(*)']

  def test_aggregate_function_with_argument_is_tokenized(self):
    tokens = self.t.parse("select count(col_a) from table_a")
    tokens.column_definitions.aggregate_functions.asList() == ['count(col_a)']

  @unittest.skip
  def test_aggregate_function_with_distinct_argument_is_tokenized(self):
    tokens = self.t.parse("select count(distinct col_a) from table_a")
    tokens.column_definitions.aggregate_functions.asList() == ['count(distinct col_a)']

