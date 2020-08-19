#!/bin/sh -v

python3 TestValidate1.py > test1.out

wc -l test1.out
sort test1.out > test2.out
wc -l test2.out

grep -v delete ../../bucket_data/dbp-prod.txt > prod1.out
wc -l prod1.out

diff -b test2.out prod1.out > test.diff
wc -l test.diff


