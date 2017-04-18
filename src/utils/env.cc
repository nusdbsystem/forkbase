// Copyright (c) 2017 The Ustore Authors.
#include "utils/env.h"

#include <gflags/gflags.h>
#include <fcntl.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <string>
#include "utils/logging.h"

namespace ustore {

// configuration file passed from the commandline
DEFINE_string(config, "../conf/config", "");

const Config* Env::GetConfig() {
  if (!config_) {
    config_ = new Config;
    int fd = open(FLAGS_config.c_str(), O_RDONLY);
    if (fd == -1) {
      LOG(INFO) << "Using default configuration (file \""
          << FLAGS_config << "\" not found)";
    } else {
      LOG(INFO) << "Load configuration \"" << FLAGS_config << "\"";
      google::protobuf::TextFormat::Parse(
          new google::protobuf::io::FileInputStream(fd), config_);
    }
    LOG(INFO) << "worker_file: " << config_->worker_file();
    LOG(INFO) << "clientservice_file = " << config_->clientservice_file();
    LOG(INFO) << "recv_threads: " << config_->recv_threads();
    LOG(INFO) << "service_threads = " << config_->service_threads();
  }
  return config_;
}
}  // namespace ustore
