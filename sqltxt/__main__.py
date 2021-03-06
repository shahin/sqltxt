"""
Translate SQL to coreutils and Bash shell commands.

Usage:
    txtsql [--debug] [-e | --execute] [--random-seed=<int>] [SQL]
    
Arguments:
    SQL         the SQL statement to translate into command line tool
                calls, e.g. cut, awk, sort, wc, etc. If none is given,
                read from stdin instead.

Options:
    --debug             output debug messages
    -e --execute        execute the resulting shell commands
    --random-seed=<int> the random seed to use for stochastic functions like TABLESAMPLE
"""

from __future__ import print_function
import sys
import os

import logging
import subprocess

from docopt import docopt

from sql_tokenizer import parse, get_relations_and_conditions
from query import Query

# unbuffer input stream to enable --execute on piped input data
stdin = os.fdopen(sys.stdin.fileno(), 'rb', 0)
sys.stdin = stdin

def main():
    args = docopt(__doc__)
    sql_str = args['SQL'] or sys.stdin.read()
    debug = args['--debug']
    execute = args['--execute']
    random_seed = args['--random-seed']
 
    if debug:
        logging.basicConfig(level=logging.DEBUG)
 
    parsed = parse(sql_str)
    relations, conditions = get_relations_and_conditions(parsed)
    sample_size = parsed.sample_size if parsed.sample_size != '' else None
 
    query = Query(
        relations, 
        conditions=conditions, 
        columns=parsed.column_definitions,
        sample_size=sample_size,
        random_seed=random_seed,
        is_top_level=True
    )
    result = query.execute()
    result_str = result.get_cmd_str(output_column_names=True)
 
    if execute:
        # explicitly use bash instead of the default for subprocess(..., shell=True) which is sh
        result_str = "({})".format(result_str)
        proc = subprocess.Popen(['/bin/bash', '-c', result_str])

    else:
        result_str = result_str + "\n"
        print(result_str, end="")


if __name__ == '__main__':
    main()

