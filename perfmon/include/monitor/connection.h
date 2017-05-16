// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#ifndef USTORE_PERFMON_MONITOR_CONNECTION_H_
#define USTORE_PERFMON_MONITOR_CONNECTION_H_

#include <inttypes.h>
#include <vector>
#include <map>
#include <string>

class Packet;
class Process;
class NetworkMonitor;

class Connection{
 public:
  explicit Connection(Packet *);
  ~Connection();

  bool includePkt(Packet *);
  void addPkt(Packet *);
  void closeCurrentSlot();
  void sum(uint64_t * sent, uint64_t * recv, size_t numSlot = 1);
  bool isAlive();

  inline Packet* getRefPkt() {
      return refPkt_;
  }

  inline uint32_t getTotalPktSent() {
      return totalPktSent_;
  }

  inline uint32_t getTotalPktRecv() {
      return totalPktRecv_;
  }

  inline void setOwner(Process * proc) {
      owner_ = proc;
  }

 private:
  Packet *refPkt_;
  Packet *nullIncomingPkt_;
  Packet *nullOutgoingPkt_;
  Packet *lastOutgoingPkt_;
  Packet *lastinComingPkt_;
  std::vector<Packet *> sentPkts_;
  std::vector<Packet *> recvPkts_;
  uint32_t totalPktSent_;
  uint32_t totalPktRecv_;
  uint32_t lastPktTime_;
  Process * owner_;
};

class ConnectionManager {
 public:
  ConnectionManager();
  // ConnectionManager(NetworkMonitor *);
  ~ConnectionManager();
  Connection *findConnection(Packet *);

  void updateMap(char *);
  void refreshMap();
  void addInterface(char *);
  uint64_t findInode(Connection *);

  inline void addConnection(Connection * conn) {
    connList_.push_back(conn);
  }

  static ConnectionManager* getConnMgr() {
    static ConnectionManager manager;
    return &manager;
  }

 private:
  std::vector<Connection*> connList_;
  std::vector<char *> ifaceName_;
  // NetworkMonitor* monitor_;
  std::map<std::string, uint64_t> connToInodeMap_;
};

#endif  // USTORE_PERFMON_MONITOR_CONNECTION_H_
