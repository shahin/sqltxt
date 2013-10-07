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

  q = Query(
        tokens.from_clauses.asList(), 
        tokens.where.asList()[0][1:], 
        tokens.column_names.asList(),
        True
      )

  result = q.generate_table()
  print(result.get_cmd_str(output_column_names=True))
  
