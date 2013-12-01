from sql_tokenizer import SqlTokenizer
from query import Query
import logging

if __name__ == '__main__':

  import argparse

  parser = argparse.ArgumentParser(description='Translate SQL to standard command line tools.')
  parser.add_argument('sql', metavar='SQL')
  parser.add_argument('--log', metavar='LOG')

  args = parser.parse_args()
  sql_str = args.sql
  loglevel = args.log

  if loglevel:
    numeric_loglevel = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_loglevel, int):
      raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(level=numeric_loglevel)

  t = SqlTokenizer()
  tokens = t.parse(sql_str)

  q = Query(
        tokens.from_clauses.asList(), 
        tokens.where.asList()[0][1:], 
        tokens.column_definitions.asList(),
        True
      )

  result = q.generate_table()
  print(result.get_cmd_str(output_column_names=True))
  
