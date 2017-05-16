// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <string>

using std::string;

int main(int argc, char *argv[]) {
  bool flags = true;
  string path = "/tmp/perfmon/pid_list";
  string file = path+"/pid_test";
  int pid = getpid();

  FILE *f = fopen(file.c_str(), "w");
  if (f == nullptr) {
    printf("cannot open file %s\n", file.c_str());
    exit(0);
  }

  printf("test running as pid = %d\n", pid);

  fprintf(f, "%d\n", pid);
  fclose(f);

  while (flags) {
    printf("test still running...\n");
    sleep(5);
  }
  return 0;
}
