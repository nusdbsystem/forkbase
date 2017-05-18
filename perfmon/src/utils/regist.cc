// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#include <unistd.h>
#include <cstdlib>
#include <cstdio>
#include <string>

#include "utils/regist.h"

string PERFMON_REG_PATH = "/tmp/perfmon/pid_list";

void setRegistPath(const char* path) {
  PERFMON_REG_PATH = string(path);
}

bool registInPerfmon(const char* filename) {
  int pid = getpid();
  string file = PERFMON_REG_PATH + "/" + filename;

  FILE *f = fopen(file.c_str(), "w");
  if (f == nullptr) {
    fprintf(stderr, "cannot open file %s\n", file.c_str());
    return false;
  }

  printf("%s running as pid = %d\n", filename, pid);
  fprintf(f, "%d\n", pid);
  fclose(f);

  return true;
}
