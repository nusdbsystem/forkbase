// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_ENV_H_
#define USTORE_UTILS_ENV_H_

#include <gflags/gflags.h>
#include "proto/config.pb.h"
#include "utils/singleton.h"

#ifndef GFLAGS_GFLAGS_H_
namespace gflags = google;
#endif  // GFLAGS_GFLAGS_H_

/**
 * This is a temporary place for storing global configuration
 * parameters, e.g. number of nodes, of threads, etc.
 * The specific values are defined in config.cc
 */
namespace ustore {

DECLARE_string(config);  // passed from the commandline

/*
 * USTORE environment
 */
class Env: public Singleton<Env>, private Noncopyable {
 public:
  const Config* GetConfig();
  void ClearConfig() {
    if (config_) {
      delete config_;
      config_ = nullptr;
    }
  }

 private:
  Config* config_ = nullptr;
};

}  // namespace ustore
#endif  // USTORE_UTILS_ENV_H_
