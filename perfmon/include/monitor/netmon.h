// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#ifndef USTORE_PERFMON_MONITOR_NETMON_H_
#define USTORE_PERFMON_MONITOR_NETMON_H_

#include <netinet/in.h>
#include <inttypes.h>
#include <vector>
#include <map>

class Interface;

class NetworkMonitor{
 public:
  NetworkMonitor();
  ~NetworkMonitor();
  void startMonitor();
  void addInterface(char *);
  bool contains(const in_addr_t &);

  static NetworkMonitor* getNetworkMonitor() {
    static NetworkMonitor monitor;
    return &monitor;
  }

 private:
  std::vector<Interface *> interfaces_;
};

#endif  // USTORE_PERFMON_MONITOR_NETMON_H_
