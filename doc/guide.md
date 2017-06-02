# UStore Usage Guide

## Preparation

Install all required [dependencies](depend.md).

## Compilation

```
  # go to UStore root dir
  mkdir build && cd build
  cmake ..
  make
```
UStore have many compilation options, which can be found in ``CMakeLits.txt``.
```
  # compile with RDMA and examples
  cmake -DUSE_RDMA=ON -DENABLE_EXAMPLE=ON ..
  make
```

## Unit Test

```
  # go to build dir
  cd build
  ./bin/test_ustore
```
Unit test data are stored in ``ustore_data/ustore_2017``.

## Micro-Benchmark

```
  ./bin/micro_bench
```
Unit benchmark data are stored in ``ustore_data/ustore_2018``.

## Setup UStore Service

All configurations can be set in ``conf/config``.
Import settings are:
  * ``num_segments``: maximum data segments a worker can store
  * ``worker_file``: a list of worker nodes
  * ``http_port``: port for default RESTful service

Start UStore service, which will launch all worker processes and a default
http server.
```
  ./bin/ustore_start.sh
```

Stop UStore service.
```
  ./bin/ustore_stop.sh
```

Or stop UStore service and remove related data.
```
  ./bin/ustore_clean.sh
```

## Distributed Benchmark

Ensure UStore service is on before running the benchmark.
Better remove benchmarked data in the end.
```
  ./bin/ustore_start.sh
  ./bin/dist_bench
  ./bin/ustore_clean.sh
```

## Commandline Client

Ensure UStore service is on.
```
  ./bin/ustore_cli
  # e.g., put a value
  ./bin/ustore_cli put -k key -b branch -v value
```
All APIs can be found in [API doc](api_cmd.md).

## Http Request

Ensure UStore service is on.
```
  # e.g., put a value
  curl http://localhost:60600/put -X POST -d "key=key&branch=branch&value=value"
```
All APIs can be found in [API doc](api_http.md).

## Performance Minotor

Please read [perfmon guide](../perfmon/README.md).
