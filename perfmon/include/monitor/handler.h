// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#ifndef USTORE_PERFMON_MONITOR_HANDLER_H_
#define USTORE_PERFMON_MONITOR_HANDLER_H_

#include <inttypes.h>
#include <netinet/ip.h>

extern "C" {
#include <pcap.h>
}

class PacketHandler;
class ConnectionManager;
class ProcessManager;
class Packet;

typedef struct pcap_pkthdr PacketHeader;
typedef void (*pktParser)
  (PacketHandler *, const PacketHeader *, const u_char *);
// typedef int (*pktHandler) (u_char *, const dp_header *, const u_char *);

typedef enum {
    PKT_ETHERNET,
    PKT_SLL,
    PKT_IP,
    PKT_TCP,
    PKT_UDP,
    PKT_TYPES
} PacketType;

typedef struct {
    int sa_family;
    in_addr src;
    in_addr dst;
} ConnAddress;

class PacketHandler{
 public:
  explicit PacketHandler(char *dev);
  ~PacketHandler();
  void startParseAndProcess(const struct pcap_pkthdr *, const u_char *);
  bool processIpPkt(const PacketHeader *, const u_char *);
  bool processTcpPkt(const PacketHeader *, const u_char *);
  bool processUdpPkt(const PacketHeader *, const u_char *);
  int openDevLive();
  int setNonBlock();
  int dispatch(int);

 private:
  pcap_t * pcapHandler_;
  pktParser pktParser_;
  uint8_t linkType_;
  uint32_t len_;
  timeval curTime_;
  char * dev_;
  ConnAddress * userData_;
  uint32_t userDataSize_;

  // methods
  int fillHandler(pcap_t *);
  void process(Packet *);
};

#endif  // USTORE_PERFMON_MONITOR_HANDLER_H_
