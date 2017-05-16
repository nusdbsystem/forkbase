// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#ifndef USTORE_PERFMON_UTILS_CONFIG_H_
#define USTORE_PERFMON_UTILS_CONFIG_H_

#include <cstdio>
#include <cstdlib>
#include <string>
#include <map>

using std::string;
using std::map;

class Config{
 public:
  void loadConfigFile(string file);
  string get(string feature);
 private:
  map<string, string> dic;
};

#endif  // USTORE_PERFMON_UTILS_CONFIG_H_
