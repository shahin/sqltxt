from sql_tokenizer import SqlTokenizer
from query import Query

if __name__ == '__main__':

  import argparse

  parser = argparse.ArgumentParser(description='Translate SQL to standard command line tools.')
  parser.add_argument('sql', metavar='SQL')

  args = parser.parse_args()
  sql_str = args.sql

  t = SqlTokenizer()
  tokens = t.parse(sql_str)
  print(tokens.from_clauses.asList())

  q = Query(tokens.column_names.asList(), tokens.column_names.asList(), tokens.where.asList()[0][1:])
  result = q.generate_table()
  print(result.cmd)
  
