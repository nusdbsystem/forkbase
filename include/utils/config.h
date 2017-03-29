// Copyright (c) 2017 The Ustore Authors.
#ifndef USTORE_UTILS_CONFIG_H_
#define USTORE_UTILS_CONFIG_H_
#include <string>
using std::string;

/**
 * This is a temporary place for storing global configuration
 * parameters, e.g. number of nodes, of threads, etc.
 * The specific values are defined in config.cc
 */
namespace ustore {

class Config {
 public:
    static const string WORKER_FILE;
    static const string CLIENTSERVICE_FILE;
    static const int RECV_THREADS;
    static const int SERVICE_THREADS;
};
}  // namespace ustore
#endif  // USTORE_UTILS_CONFIG_H_
