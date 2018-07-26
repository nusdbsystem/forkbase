#!/usr/bin/env bash

USTORE_CLEAN="./bin/ustore_clean.sh"
USTORE_START="./bin/ustore_start.sh"
USTORE_CLI="./bin/ustore_cli"
USTORE_LUCENE_CLI="./bin/ustore_lucene_cli"

echo [Clean Data] ...
$USTORE_CLEAN

echo [Start UStore] ...
$USTORE_START

echo [Create CSV] ...
echo "KEY,CA,CB,CC
k1,aa,ab,ac
k2,ba,bb,aa
k3,ca,cb,cc" > in1.csv
echo "KEY,CA,CB,CC
k4,aa,ab,ac
k5,ba,bb,bc
k6,ca,cb,ba" > in2.csv

echo [Create Dataset] ...
$USTORE_CLI create-ds -t ds_test -b master

echo [First Commit] ...
# $USTORE_LUCENE_CLI put-de-by-csv in1.csv -t ds_test -b master -i 0 -j "1,3"
$USTORE_LUCENE_CLI put-de-by-csv in1.csv -t ds_test -b master -i 0

echo [First Dump] ...
$USTORE_CLI export-ds-bin -t ds_test -b master out1.csv

echo [Second Commit] ...
# $USTORE_LUCENE_CLI put-de-by-csv in2.csv -t ds_test -b master -i 0 -j "1,3"
$USTORE_LUCENE_CLI put-de-by-csv in2.csv -t ds_test -b master -i 0

echo [Second Dump] ...
$USTORE_CLI export-ds-bin -t ds_test -b master out2.csv

echo [Query aa] ...
$USTORE_LUCENE_CLI get-de-by-iq -t ds_test -b master -q "aa"
$USTORE_LUCENE_CLI get-de-by-iq -t ds_test -b master -q "aa" q1.csv

echo [Query aa AND ac] ...
$USTORE_LUCENE_CLI get-de-by-iq -t ds_test -b master -q "aa AND ac"
$USTORE_LUCENE_CLI get-de-by-iq -t ds_test -b master -q "aa AND ac" q2.csv

# echo [Query aa OR cc] ...
echo [Query bb OR cc] ...
# $USTORE_LUCENE_CLI get-de-by-iq -t ds_test -b master -q "aa OR cc"
# $USTORE_LUCENE_CLI get-de-by-iq -t ds_test -b master -q "aa OR cc" q3.csv
$USTORE_LUCENE_CLI get-de-by-iq -t ds_test -b master -q "bb OR cc"
$USTORE_LUCENE_CLI get-de-by-iq -t ds_test -b master -q "bb OR cc" q3.csv

echo [Query CC : aa] ...
$USTORE_LUCENE_CLI get-de-by-iq -t ds_test -b master -q "CC : aa"
$USTORE_LUCENE_CLI get-de-by-iq -t ds_test -b master -q "CC : aa" q4.csv
