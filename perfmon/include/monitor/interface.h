// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#ifndef USTORE_PERFMON_MONITOR_INTERFACE_H_
#define USTORE_PERFMON_MONITOR_INTERFACE_H_

#include <netinet/in.h>
#include <inttypes.h>

class PacketHandler;

class Interface{
 public:
  Interface() {}
  ~Interface();
  bool contains(const in_addr_t &);
  int initInterface(const char *dev);
  char *getAddr();
  int processNextPkt();

 private:
  char *dev_;
  uint16_t saFamily_;
  in_addr_t addr_;
  char * str_;
  PacketHandler *handler_;
};

#endif  // USTORE_PERFMON_MONITOR_INTERFACE_H_
