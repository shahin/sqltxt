"""
Translate SQL to standard command line tools.

Usage:
    txtsql [--debug] SQL
    
Arguments:
    SQL         the SQL statement to translate into command line tool
                calls, e.g. cut, awk, sort, wc, etc.

Options:
    --debug     output debug messages
"""
import logging
from docopt import docopt

from sql_tokenizer import select_stmt
from query import Query

if __name__ == '__main__':

  args = docopt(__doc__)
  sql_str = args['SQL']
  debug = args['--debug']

  if debug:
    logging.basicConfig(level=logging.DEBUG)

  tokens = select_stmt.parseString(sql_str)

  q = Query(
        tokens.from_clause, 
        tokens.where_conditions, 
        tokens.column_definitions,
        True
      )

  result = q.generate_table()
  print(result.get_cmd_str(output_column_names=True))
  
