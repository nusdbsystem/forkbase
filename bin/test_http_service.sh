#!/usr/bin/env bash

BASE_URL=http://127.0.0.1:60600
# put
echo [Put Key] ...
curl $BASE_URL/put -X POST -d "key=http&branch=master&value=first"
# list keys
echo [List Keys] ...
curl $BASE_URL/list -X GET
# list branches
echo [List Branches] ...
curl $BASE_URL/list -X POST -d "key=http"
# get
echo [Get Key] ...
curl $BASE_URL/get -X POST -d "key=http&branch=master"
# get with xml format
echo [Get Key with XML] ...
curl $BASE_URL/get -X POST -d "key=http&branch=master" -H "accept: application/xml"
# get with json format
echo [Get Key with JSON] ...
curl $BASE_URL/get -X POST -d "key=http&branch=master" -H "accept: application/json"
