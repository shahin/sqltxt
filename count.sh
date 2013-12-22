#!/bin/bash

# select col_a, count(col_b) from table group by col_a;

join -a 2 count_col_b_group_by_col_a_fifo group_by_col_a_fifo | \
awk -F' ' '{ if($2=="") $2=0; print}' &

echo -e "1,\n2,r\n2,b\n3,b" |\
tee >(\
  awk -F',' '{ if($2 != "") print }' |\
  cut -d, -f1 |\
  sort -t, -k 1.1 |\
  uniq -c |\
  awk -F' ' '{ print $2, $1 }' \
  > count_col_b_group_by_col_a_fifo) |\
cut -d, -f1 |\
sort -t, -k 1.1 -u \
> group_by_col_a_fifo
