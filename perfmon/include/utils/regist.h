// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#ifndef USTORE_PERFMON_UTILS_REGIST_H_
#define USTORE_PERFMON_UTILS_REGIST_H_

#include <cstdlib>
#include <string>

using std::string;

void setRegistPath(const char* path);
bool registInPerfmon(const char* filename);

#endif  // USTORE_PERFMON_UTILS_REGIST_H_
