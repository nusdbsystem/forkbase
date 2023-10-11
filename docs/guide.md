# ForkBase Usage Guide

## Preparation

Install all required [dependencies](depend.md).

## Compilation

```console
# Go to ForkBase root dir
$ mkdir build && cd build
$ cmake ..
$ make
```
ForkBase has many compilation options, which can be found in ``CMakeList.txt``.
```console
# compile with RDMA and examples
$ cmake -DUSE_RDMA=ON -DENABLE_EXAMPLE=ON ..
$ make
```

## Unit Test

```console
# go to build dir
$ cd build
$ ./bin/test_ustore
```
Unit test data are stored in ``ustore_data/ustore_2017``.

## Micro-Benchmark

```console
$ ./bin/micro_bench
```
Unit benchmark data are stored in ``ustore_data/ustore_2018``.

## Setup ForkBase Service

All configurations can be set in ``conf/config``.
Import settings are:
  * ``num_segments``: maximum data segments a worker can store
  * ``worker_file``: a list of worker nodes
  * ``http_port``: port for default RESTful service

Start ForkBase service, which will launch all worker processes and a default
HTTP server.
```console
$ ./bin/ustore_start.sh
```

Stop ForkBase service.
```console
$ ./bin/ustore_stop.sh
```

Or stop ForkBase service and remove related data.
```console
$ ./bin/ustore_clean.sh
```

## Distributed Benchmark

Ensure ForkBase service is on before running the benchmark.
Better remove benchmarked data in the end.
```console
$ ./bin/ustore_start.sh
$ ./bin/dist_bench
$ ./bin/ustore_clean.sh
```

## Commandline Client

Ensure ForkBase service is on.
```console
$ ./bin/ustore_cli
# e.g., put a value
$ ./bin/ustore_cli put -k key -b branch -v value
```
All APIs can be found in [API doc](api_cmd.md).

## HTTP Request

Ensure ForkBase service is on.
```console
# e.g., put a value
$ curl http://localhost:60600/put -X POST -d "key=key&branch=branch&value=value"
```
All APIs can be found in [API doc](api_http.md).
