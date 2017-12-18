## Build ustore go wrapper

### Prerequisites
+ Go 1.8.3 or after
+ `GOPATH` is set. All Go packages will be stored at `$GOPATH/src`

### Build with UStore
+ Build UStore with cmake, while setting `ENABLE_GO` option to be `ON` 
+ If successful, the Go binding is generated at `$GOPATH/src/ustore`

### Test
+ Go to `go/unit_test`
+ `LD_LIBRARY_PATH=<path to libustore_kv.so>:$LD_LIBRARY_PATH go test -v`

