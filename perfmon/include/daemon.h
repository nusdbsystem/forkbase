// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc 
// Modified by: zl

#ifndef USTORE_PERFMON_DAEMON_H_
#define USTORE_PERFMON_DAEMON_H_

#include <cstdio>
#include "utils/sock.h"
#include "utils/protobuf.h"
#include "./perfmon.h"

class PerfmonDaemon{
 public:
  // constant
  static constexpr int BUF_LEN = PerformanceMonitor::BUF_LEN;
  static constexpr int UNIT = sizeof(struct ProcInfo);
  // http server
  int http_port;
  ProtoBuffer buffer;

  PerfmonDaemon(int monitor_port, int ui_port);
  ~PerfmonDaemon();
  void start();

 private:
  // socket
  int sock_port;
  SocketServer socket;
  char buf[BUF_LEN];

  void processMessage(const char* hostname, int len);
};

#endif  // USTORE_PERFMON_DAEMON_H_
