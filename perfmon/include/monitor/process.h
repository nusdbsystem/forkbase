// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#ifndef USTORE_PERFMON_MONITOR_PROCESS_H_
#define USTORE_PERFMON_MONITOR_PROCESS_H_

#include <inttypes.h>
#include <sys/time.h>

#include <string>
#include <vector>
#include <map>

#define MAXSLOTKEPT 300  // resource usage of recent 300 slots will be kept

class ProcessManager;
class Connection;
class Packet;

class Process{
  friend class ProcessManager;
 public:
  Process(int pid, std::string pname);
  ~Process();

  Connection* findConnection(Packet *);
  void addConnection(Connection *);
  void updateSystemUsage();
  int updateNetworkSocket();

 private:
  int pid_;
  std::string name_;
  uint64_t startTime_;

  // resource usage
  uint64_t utime_;                // ser time
  uint64_t stime_;                // ernel time
  uint64_t rssSize_;              // esident set size
  uint64_t vmSize_;               // irtual memory size
  uint64_t bytesRead_;             // otal bytes read
  uint64_t bytesWritten_;          // otal bytes written
  uint64_t bytesReadFromDisk_;     // ytes read from disk
  uint64_t bytesWrittenToDisk_;    // ytes written to disk
  uint64_t bytesSent_;            // ytes sent
  uint64_t bytesRecv_;            // ytes recv'd

  // metrics read last time
  uint64_t lastUtime_;
  uint64_t lastStime_;
  // int64_t lastRssSize_;
  // int64_t lastVmSize_;
  uint64_t lastBytesRead_;
  uint64_t lastBytesWritten_;
  uint64_t lastBytesReadFromDisk_;
  uint64_t lastBytesWrittenToDisk_;
  // int64_t lastBytesSent_;
  // int64_t lastBytesRecv_;

  std::vector<Connection *> connList_;  // the connections of me

  static ProcessManager * procMgr_;

  // truct timeval lastSlotTime_;
  void countSystemUsage();
  void countIoUsage();
  void countNetworkUsage();
};

class ProcessManager{
 public:
  ProcessManager();
  ~ProcessManager();

  void updateProcessList(std::map<int, std::string>& pidList);
  void transferConnection(Process *);
  void countCpuUsage();
  void countSlotLen();
  void printProcessInfo();
  int printProcessInfoToBuf(char* buf);

  void refreshMap();
  int refresh(std::map<int, std::string>& pidList, char * buf);
  Process* findProcess(uint64_t);
  bool containMap(uint64_t, Process*);
  inline void updateMap(uint64_t inode, Process* proc) {
      inodeToProcMap_[inode] = proc;
  }

  // sigleton
  static ProcessManager* getProcMgr() {
      static ProcessManager manager;
      return &manager;
  }

 private:
  struct timeval lastSlotTime_;
  uint64_t cpuTime_;
  uint64_t lastCpuTime_;
  double lastSlotLen_;

  std::vector<Process *> procList_;
  Process * unknownProc_;
  std::map<uint64_t, Process *> inodeToProcMap_;
};

#endif  // USTORE_PERFMON_MONITOR_PROCESS_H_

