// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#ifndef USTORE_PERFMON_PERFMON_H_
#define USTORE_PERFMON_PERFMON_H_

#include <cstdio>
#include <map>
#include <string>
#include "utils/sock.h"
#include "utils/proto.h"

class NetworkMonitor;
class ProcessManager;

class PerformanceMonitor{
 public:
  // constant
  static constexpr int MAX_PROC = 50;
  static constexpr int BUF_LEN = (MAX_PROC+1)*(sizeof(struct ProcInfo));

  PerformanceMonitor(const char * const hostname, int port,
    char* const dir, int numNic = 0, char** nicList = NULL);
  ~PerformanceMonitor();
  void start();
 private:
  // socket
  SocketClient socket;
  char buf[BUF_LEN];
  // monitor
  char pidFile[BUF_LEN];

  char* dir_;
  void refreshProcess();
  map<int, string> pidList_;
};

#endif  // USTORE_PERFMON_PERFMON_H_

