// Copyright (c) 2017 The Ustore Authors.
#include "utils/env.h"

#include <gflags/gflags.h>
#include <fcntl.h>
#include <unistd.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <string>
#include "utils/logging.h"

namespace ustore {

// configuration file passed from the commandline
DEFINE_string(config, Env::kDefaultConfigFile, "");

const char* Env::kDefaultConfigFile = "conf/config";

Env::Env() {
  int fd = open(FLAGS_config.c_str(), O_RDONLY);
  if (fd == -1) {
    LOG(WARNING) << "Using default configuration (file \""
                 << FLAGS_config << "\" not found)";
    FLAGS_config = kDefaultConfigFile;
    fd = open(FLAGS_config.c_str(), O_RDONLY);
    if (fd == -1) {
      LOG(FATAL) << "Fail to load default configuration (file \""
                 << FLAGS_config << "\" not found)";
      return;
    }
  }
  LOG(INFO) << "Load configuration \"" << FLAGS_config << "\"";
  google::protobuf::TextFormat::Parse(
      new google::protobuf::io::FileInputStream(fd), &config_);
  LOG(INFO) << "Loaded configuration:" << std::endl << config_.DebugString();
  close(fd);
}
}  // namespace ustore
