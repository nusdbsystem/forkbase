// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#ifndef USTORE_PERFMON_UTILS_PROTO_H_
#define USTORE_PERFMON_UTILS_PROTO_H_

struct ProcInfo{
  // ame of the process
  char name[24];
  // pu utilization in percentage
  double cpu;
  // emory of virtual and rss
  unsigned long mem_v;
  unsigned long mem_r;
  // isk usage of read and write
  int io_read;
  int io_write;
  // etwork usage of send and receive
  int net_send;
  int net_recv;
};

#endif  // USTORE_PERFMON_UTILS_PROTO_H_
