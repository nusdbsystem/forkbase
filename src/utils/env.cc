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
  const char* home_path = getenv("USTORE_HOME");
  std::string config_path = kDefaultConfigFile;
  if (home_path == nullptr)
    LOG(WARNING) << "Use working dir (env USTORE_HOME not set)";
  else
    config_path = home_path + std::string("/") + config_path;
  int fd = open(config_path.c_str(), O_RDONLY);
  if (fd == -1) {
    LOG(FATAL) << "Fail to load configuration (file \""
               << config_path << "\" not found)";
    return;
  }
  LOG(INFO) << "Load config \"" << config_path << "\"";
  google::protobuf::TextFormat::Parse(
      new google::protobuf::io::FileInputStream(fd), &config_);
  LOG(INFO) << "Loaded config:" << std::endl << config_.DebugString();
  close(fd);
}
}  // namespace ustore
