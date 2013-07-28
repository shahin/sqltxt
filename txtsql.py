from query import Query

if __name__ == '__main__':

  import argparse

  parser = argparse.ArgumentParser(description='Translate SQL to standard command line tools.')
  parser.add_argument('sql', metavar='SQL')

  args = parser.parse_args()
  sql_str = args.sql

  q = Query(sql_str)
  result = q.generate_table()
  print(result.cmd)
  
