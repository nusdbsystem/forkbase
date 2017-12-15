// Copyright (c) 2017 The Ustore Authors.
#include "utils/env.h"

#include <fcntl.h>
#include <unistd.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <cstdlib>
#include <string>
#include "utils/logging.h"

namespace ustore {

const char* Env::kDefaultConfigFile = "conf/config.cfg";

Env::Env() {
  const char* config_path = getenv("USTORE_CONF");
  int fd = open(config_path, O_RDONLY);
  if (fd == -1) {
    if (config_path == nullptr)
      LOG(WARNING) << "Use default configuration (env USTORE_CONF not set)";
    else
      LOG(WARNING) << "Use default configuration (file \""
                   << config_path << "\" not found)";
    config_path = kDefaultConfigFile;
    fd = open(config_path, O_RDONLY);
    if (fd == -1) {
      LOG(FATAL) << "Fail to load default configuration (file \""
                 << config_path << "\" not found)";
      return;
    }
  }
  LOG(INFO) << "Load configuration \"" << config_path << "\"";
  google::protobuf::TextFormat::Parse(
      new google::protobuf::io::FileInputStream(fd), &config_);
  LOG(INFO) << "Loaded configuration:" << std::endl << config_.DebugString();
  close(fd);
}
}  // namespace ustore
