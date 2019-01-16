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
# joined table
echo "KID, JA, JB, J C
k1,aa,ja,j1
k2,ba,jb,j2
k3,ca,jc,j3
k4,aa,jd,j4
k5,ba,je,j5
k6,ca,jf,j6" > join.csv

echo [Create Dataset] ...
$USTORE_CLI create-ds -t simple_key -b master
$USTORE_CLI create-ds -t composed_key -b master
# for joined tables
$USTORE_CLI create-ds -t simple_join -b master
$USTORE_CLI create-ds -t composed_join -b master

echo [First Commit] ...
$USTORE_LUCENE_CLI put-de-by-csv init.csv -t simple_key -b master -m 0
$USTORE_LUCENE_CLI put-de-by-csv init.csv -t composed_key -b master -m 0,1

echo [First Dump] ...
$USTORE_CLI export-ds-bin -t simple_key -b master out-init-s.csv
$USTORE_CLI export-ds-bin -t composed_key -b master out-init-c.csv
diff out-init-s.csv out-init-c.csv

echo [Second Commit] ...
$USTORE_LUCENE_CLI put-de-by-csv delta.csv -t simple_key -b master -m 0
$USTORE_LUCENE_CLI put-de-by-csv delta.csv -t composed_key -b master -m 0,1
$USTORE_CLI put-de-by-csv join.csv -t simple_join -b master -m 0 --with-schema
$USTORE_CLI put-de-by-csv join.csv -t composed_join -b master -m 0,1 --with-schema

echo [Second Dump] ...
$USTORE_CLI export-ds-bin -t simple_key -b master out-delta-s.csv
$USTORE_CLI export-ds-bin -t composed_key -b master out-delta-c.csv
diff out-delta-s.csv out-delta-c.csv

echo [Check Schema] ...
$USTORE_LUCENE_CLI get-ds-sch -t simple_key -b master
$USTORE_LUCENE_CLI get-ds-sch -t composed_key -b master

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

echo [Joined Query aa] expected result size is 3 ...
rm -rf simple_dir
$USTORE_LUCENE_CLI get-de-by-iqj -t simple_key -b master -q "aa" -s "simple_key,simple_join" simple_dir
rm -rf composed_dir
$USTORE_LUCENE_CLI get-de-by-iqj -t composed_key -b master -q "bb OR cc" -s "composed_key,composed_join" composed_dir
