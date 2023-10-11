# ForkBase

ForkBase is a distributed data storage system with rich semantics and features that unifies and adds values to many classes of next-generation applications. In particular, ForkBase is mainly designed for applications that require internal data representation with versions, chains, and branches. Typical applications include Git-like versioning control, Blockchain, Collaborative analytics, and versioned OLTP. ForkBase focuses on providing such applications with high performance, flexibility, and high-level semantics while reducing application-level development and maintenance effort.

* [Features](docs/intro.md)
* [Architecture](docs/arch.md)

## Dependency

### Compulsory

* cmake >= 2.8
* g++ >= 4.9
* protobuf >= 2.6.1
* boost >= 1.66.0
* czmq >= 4.0.2
* libzmq >= 4.2.1
* gflags >= 2.1.0

### Optional

#### For RDMA

* libibverbs >= 1.0
* libboost_thread >= 1.5.4 (Thread)

#### For Cryptographic Hashing

* libcrypto++

#### For Storage

* RocksDB >= 5.8
* LevelDB
* Snappy

## Getting Started

### Build ForkBase
```console
$ mkdir build
$ cd build
$ cmake .. && make
```

### Start ForkBase
```console
$ ./bin/ustore_start.sh
```

### Stop ForkBase
```console
# stop the service
$ ./bin/ustore_stop.sh

# stop the service and clean the data
$ ./bin/ustore_clean.sh
```

### References
* [Usage Guide](docs/guide.md)
* [Commandline Console API](docs/api_cmd.md)
* [Http RESTful API](docs/api_http.md)

## Contribute
Please follow our [Development Guidelines](docs/pr_guide.md) to submit pull requests.

## Citation
If you use our code in your research, please kindly cite:
```
@article{10.14778/3231751.3231762,
  author = {Wang, Sheng and
            Dinh, Tien Tuan Anh and
            Lin, Qian and
            Xie, Zhongle and
            Zhang, Meihui and
            Cai, Qingchao and
            Chen, Gang and
            Ooi, Beng Chin and
            Ruan, Pingcheng},
  title = {Forkbase: An Efficient Storage Engine for Blockchain and Forkable Applications},
  journal = {Proc. VLDB Endow.},
  volume = {11},
  number = {10},
  pages = {1137--1150},
  year = {2018}
}
```

## Contact
To ask questions or report issues, please drop us an [email](mailto:yuecong@comp.nus.edu.sg).
