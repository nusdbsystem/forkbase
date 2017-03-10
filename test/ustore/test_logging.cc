// Copyright (c) 2017 The Ustore Authors.
#include "utils/logging.h"
#include "gtest/gtest.h"

TEST(Logging, InfoLogging) {
  ustore::InitLogging("");
  int a = 3;
  CHECK_EQ(a, 3);
  LOG(INFO) << "test info logging";
}

TEST(Logging, WarningLogging) {
  int a = 4;
  CHECK_EQ(a, 4);
  LOG(WARNING) << "test warning logging";
}

TEST(Logging, ErrorLogging) {
  int a = 5;
  CHECK_EQ(a, 5);
  LOG(ERROR) << "test error logging";
}

/*
TEST(Logging, FatalLogging) {
  int a = 6;
  CHECK_EQ(a, 6);
  LOG(FATAL) << "test fatal logging";
}
*/

TEST(Logging, SetLogDestination) {
  int a = 6;
  ustore::SetLogDestination(ustore::WARNING, "/tmp/test.log");
  CHECK_EQ(a, 6);
  LOG(WARNING) << "test warning logging to file";
}

TEST(Logging, StderrLoggingLevel) {
  int a = 6;
  ustore::SetStderrLogging(ustore::WARNING);
  CHECK_EQ(a, 6);
  LOG(INFO) << "test info logging to stderr";
  LOG(WARNING) << "test warning logging to stderr and file";
  LOG(ERROR) << "test error logging to stderr and file";
}
