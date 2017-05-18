// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#ifndef USTORE_PERFMON_MONITOR_PACKET_H_
#define USTORE_PERFMON_MONITOR_PACKET_H_

#include <inttypes.h>
#include <net/ethernet.h>
#include <sys/socket.h>
#include <arpa/inet.h>

typedef enum {
    UNKNOWN,
    OUTGOING,
    INCOMING
} Direction;

#define CONNSTRLEN 92

class Packet{
 public:
  // Packet (in_addr, uint16_t, in_addr,
  //   uint16_t, uint32_t, timeval, Direction);
  Packet(in_addr sip, uint16_t sport, in_addr dip,
    uint16_t dport, uint32_t len, struct timeval time,
    Direction direction = UNKNOWN);
  explicit Packet(const Packet &);
  ~Packet();

  inline uint32_t getLength() {return this->len_;}
  inline void addLength(uint32_t len) {
      len_ += len;
  }

  char *getConnStr();

  Packet *newInverted();

  bool isOlderThan(timeval);

  bool isOutgoing();

  // check if this packet belongs to the same connection as the parameter
  bool match(Packet *);

  static Packet* getNullIncomingPkt(Packet *);
  static Packet* getNullOutgoingPkt(Packet *);

 private:
  Direction direction_;
  uint16_t saFamily_;
  char * connStr_;  // string representation of connection
  in_addr sip_;
  in_addr dip_;
  uint16_t sport_;
  uint16_t dport_;
  uint32_t len_;
  struct timeval time_;
};

#endif  // USTORE_PERFMON_MONITOR_PACKET_H_

