// Copyright (c) 2017 The Ustore Authors.

#include <stdio.h>
#include <fstream>
#include <string>
#include <cstring>
#include "gtest/gtest.h"
#include "utils/env.h"
#include "utils/logging.h"

using ustore::Env;
using ustore::Config;

const std::string kConfigArg = std::string("--config=")
                               + Env::kDefaultConfigFile;
const std::string kDefaultWorkerFile = "conf/workers";
const int kDefaultRecvThreads = 1;

TEST(ConfigTest, ConfigFile) {
  int argc = 2;
  char** argv = new char*[argc];
  argv[1] = new char[kConfigArg.length()+1];  // arguments starting at index 1
  memcpy(argv[1], kConfigArg.c_str(), kConfigArg.length()+1);

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  CHECK_EQ(ustore::FLAGS_config, Env::kDefaultConfigFile);
}

TEST(ConfigTest, ConfigParse) {
  // load the config file
  const Config& c = Env::Instance()->config();
  CHECK_EQ(c.worker_file(), kDefaultWorkerFile);
  CHECK_EQ(c.recv_threads(), kDefaultRecvThreads);  // default value
}
