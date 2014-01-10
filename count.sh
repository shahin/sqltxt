#!/bin/bash

# this is what I want sqltxt.py to generate for the given SQL:
# select col_a, count(col_b) from table group by col_a;

SQLTXT_TMPDIR=$(mktemp -d -t sqltxt_); mkfifo $SQLTXT_TMPDIR/count_col_b_group_by_col_a $SQLTXT_TMPDIR/group_by_col_a || exit 1;

join -a 2 $SQLTXT_TMPDIR/count_col_b_group_by_col_a $SQLTXT_TMPDIR/group_by_col_a | \
awk -F' ' '{ if($2=="") $2=0; print}' &

echo -e "1,\n2,r\n2,b\n3,b" |\
tee >(\
  awk -F',' '{ if($2 != "") print }' |\
  cut -d, -f1 |\
  sort -t, -k 1.1 |\
  uniq -c |\
  awk -F' ' '{ print $2, $1 }' \
  > $SQLTXT_TMPDIR/count_col_b_group_by_col_a) |\
cut -d, -f1 |\
sort -t, -k 1.1 -u \
> $SQLTXT_TMPDIR/group_by_col_a;

rm -rf $SQLTXT_TMPDIR;
unset SQLTXT_TMPDIR;
