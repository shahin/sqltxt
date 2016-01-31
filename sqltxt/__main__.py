"""
Translate SQL to coreutils and Bash shell commands.

Usage:
    txtsql [--debug] [-e | --execute] SQL
    
Arguments:
    SQL         the SQL statement to translate into command line tool
                calls, e.g. cut, awk, sort, wc, etc.

Options:
    --debug         output debug messages
    -e --execute    execute the resulting shell commands
"""
import logging
import subprocess
from docopt import docopt

from sql_tokenizer import parse, get_relations_and_conditions
from query import Query

def main():
    args = docopt(__doc__)
    sql_str = args['SQL']
    debug = args['--debug']
    execute = args['--execute']
 
    if debug:
        logging.basicConfig(level=logging.DEBUG)
 
    parsed = parse(sql_str)
    relations, conditions = get_relations_and_conditions(parsed)
 
    query = Query(
        relations, 
        conditions, 
        parsed.column_definitions,
        True
    )
    result = query.execute()
    result_str = result.get_cmd_str(output_column_names=True)
 
    if execute:
        # explicitly use bash instead of the default for subprocess(..., shell=True) which is sh
        result_str = subprocess.check_output(['/bin/bash', '-c', result_str])

    print(result_str)


if __name__ == '__main__':
    main()

