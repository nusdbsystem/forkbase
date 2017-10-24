## Build wrapper for ustore
0. cmake > 3.0
1. build the ustore
2. Install ustore_kv.so so that linker can find it, e.g,
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/ruanpingcheng/Desktop/USTORE-1/build/lib
3. go to $GO_PATH/src/ustore to test 
```
go test
```

## To run Hyperledger with Ustore
0. go version = 1.7.6 or 1.8.3
1. go get github.com/ijingo/fabric

### For testing
Set the stateDatabase option in fabric/core/ledger/kvledger/pkg_test.go

```go test
```
NOTE: Need to copy ustore/conf to directory running the binary

### For running
 Set stateDatabase to UStore on Line 296 in peer/core.yaml
NOTE: Need to copy ustore/conf to directorygit  running the binary

### Major Update
* fabric/core/ledger/kvledger/txmgmt/statedb/stateustore
* fabric/core/ledger/util/ustorehelper
* fabric/core/ledger/ledgerconfig/ledger_config.go

## To run Ethereum with UStore
cannot use go get github.com/ijingo/go-ethereum
must use go get github.com/etheruem/go-ethereum then add remote wj xxxx, then checkout to branch 'ustore'
work under path github.com/etheruem/go-ethereum

0. Edit ustore path at line 25 in go-ethereum/build/env.sh, e.g,
    USTORE_PATH="/home/ruanpingcheng/Desktop/USTORE-1"
1. Copy ustore directory(go) under ethereum/vendor
2. 
export CPLUS_INCLUDE_PATH=/home/ruanpingcheng/Desktop/USTORE-1/include:/home/ruanpingcheng/Desktop/USTORE-1/go/kvdb/include:$CPLUS_INCLUDE_PATH
3. Build ethereum via command
```make all``` at ethereum root directory

### For Running
4. ```cd build/bin/```
5. Copy the ustore conf directory under build/bin
6. Run ```./geth --dev console``` to start the console
NOTE: Need to copy ustore/conf to directory running the binary





