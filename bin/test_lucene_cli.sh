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
# support space in attribute name
echo "KID,CA,CB,C C
k1,aa,ab,ac
k2,ba,bb,aa
k3,ca,cb,cc" > init.csv
# support irregular schema
echo "KID, CA, CB, C  C
k4,aa,ab,ac
k5,ba,bb,bc
k6,ca,cb,ba" > delta.csv

echo [Create Dataset] ...
$USTORE_CLI create-ds -t simple_key -b master
$USTORE_CLI create-ds -t composed_key -b master

echo [First Commit] ...
$USTORE_LUCENE_CLI put-de-by-csv init.csv -t simple_key -b master -m 0
$USTORE_LUCENE_CLI put-de-by-csv init.csv -t composed_key -b master -m "0,1"

echo [First Dump] ...
$USTORE_CLI export-ds-bin -t simple_key -b master out-init-s.csv
$USTORE_CLI export-ds-bin -t composed_key -b master out-init-c.csv
diff out-init-s.csv out-init-c.csv

echo [Second Commit] ...
$USTORE_LUCENE_CLI put-de-by-csv delta.csv -t simple_key -b master -m 0 -n "0,1,2,3"
$USTORE_LUCENE_CLI put-de-by-csv delta.csv -t composed_key -b master -m "0,1" -n "0,1,2,3"

echo [Second Dump] ...
$USTORE_CLI export-ds-bin -t simple_key -b master out-delta-s.csv
$USTORE_CLI export-ds-bin -t composed_key -b master out-delta-c.csv
diff out-delta-s.csv out-delta-c.csv

echo [Query aa] expected result size is 3 ...
#$USTORE_LUCENE_CLI get-de-by-iq -t simple_key -b master -q "aa"
$USTORE_LUCENE_CLI get-de-by-iq -t simple_key -b master -q "aa" q1-s.csv
#$USTORE_LUCENE_CLI get-de-by-iq -t composed_key -b master -q "aa"
$USTORE_LUCENE_CLI get-de-by-iq -t composed_key -b master -q "aa" q1-c.csv

echo [Query aa AND ac] expected result size is 2 ...
#$USTORE_LUCENE_CLI get-de-by-iq -t simple_key -b master -q "aa AND ac"
$USTORE_LUCENE_CLI get-de-by-iq -t simple_key -b master -q "aa AND ac" q2-s.csv
#$USTORE_LUCENE_CLI get-de-by-iq -t composed_key -b master -q "aa AND ac"
$USTORE_LUCENE_CLI get-de-by-iq -t composed_key -b master -q "aa AND ac" q2-c.csv

# echo [Query aa OR cc] ...
echo [Query bb OR cc] expected result size is 3 ...
#$USTORE_LUCENE_CLI get-de-by-iq -t simple_key -b master -q "bb OR cc"
$USTORE_LUCENE_CLI get-de-by-iq -t simple_key -b master -q "bb OR cc" q3-s.csv
#$USTORE_LUCENE_CLI get-de-by-iq -t composed_key -b master -q "bb OR cc"
$USTORE_LUCENE_CLI get-de-by-iq -t composed_key -b master -q "bb OR cc" q3-c.csv

echo [Query C_C : aa] expected result size is 1 ...
#$USTORE_LUCENE_CLI get-de-by-iq -t simple_key -b master -q "C_C : aa"
$USTORE_LUCENE_CLI get-de-by-iq -t simple_key -b master -q "C_C : aa" q4-s.csv
#$USTORE_LUCENE_CLI get-de-by-iq -t composed_key -b master -q "C_C : aa"
$USTORE_LUCENE_CLI get-de-by-iq -t composed_key -b master -q "C_C : aa" q4-c.csv
