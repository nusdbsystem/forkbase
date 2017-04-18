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

const std::string kConfigFile = "test_config";
const std::string kConfigArg = "--config="+kConfigFile;
const std::string kWorkerFile = "test_worker_file";
const std::string kClientServiceFile = "test_clientservice_file";

const std::string kDefaultWorkerFile = "../conf/workers";
const std::string kDefaultClientServiceFile = "../conf/client_services";
const int kDefaultRecvThreads = 2;
const int kDefaultServiceThreads = 1;

TEST(ConfigTest, ConfigFile) {
  int argc = 2;
  char** argv = new char*[argc];
  argv[1] = new char[kConfigArg.length()];  // arguments starting at index 1
  memcpy(argv[1], kConfigArg.c_str(), kConfigArg.length());

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  CHECK_EQ(ustore::FLAGS_config, kConfigFile);
}

TEST(ConfigTest, ConfigParse) {
  // first clear the configuration
  Env::Instance()->ClearConfig();

  // prepare the config file
  ustore::FLAGS_config = kConfigFile;
  std::ofstream ofs(ustore::FLAGS_config);
  ofs << "worker_file: \"" << kWorkerFile << "\"" << std::endl;
  ofs << "clientservice_file: \"" << kClientServiceFile << "\"" << std::endl;
  ofs.close();

  // load the config file
  const Config* c = Env::Instance()->GetConfig();
  CHECK_EQ(c->worker_file(), kWorkerFile);
  CHECK_EQ(c->clientservice_file(), kClientServiceFile);
  CHECK_EQ(c->recv_threads(), kDefaultRecvThreads);  // default value
  CHECK_EQ(c->service_threads(), kDefaultServiceThreads);  // default value

  // delete the config file
  if (remove(ustore::FLAGS_config.c_str())) {  // error
    LOG(WARNING) << "delete test config file failed";
  }

  // clear the configuration again
  Env::Instance()->ClearConfig();
}
