# sqltxt
An SQL-to-coreutils intepreter for text data munging, written in Python.

## Overview
sqltxt will parse a (*very* limited) subset of SQL and translate it into functionally equivalent coreutils and awk calls.

## Requirements

* Python 2.7
* GNU coreutils (to run the interpreted output)
* awk

## Install

```bash
git clone https://github.com/shahin/sqltxt.git
cd sqltxt
pip install -e.
```

## Run

```bash
# sqltxt "
select
  table_a.col_a,
  table_b.col_z
from
  tests/data/table_a.txt table_a
  join tests/data/table_b.txt table_b on (table_a.col_a = table_b.col_a)
where
  table_b.col_z = 'w'
"| bash
col_a,col_z
1,w
2,x
2,y
