// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#include <stdlib.h>
#include <stdio.h>
#include <cstring>

#include "monitor/packet.h"
#include "monitor/netmon.h"
#include "monitor/config.h"

// extern Interface *interfaces;

Packet::Packet(in_addr sip, uint16_t sport, in_addr dip,
  uint16_t dport, uint32_t len, struct timeval time, Direction direction) {
    this->sip_ = sip;
    this->sport_ = sport;
    this->dip_ = dip;
    this->dport_ = dport;
    this->len_ = len;
    this->time_ = time;
    this->direction_ = direction;

    this->saFamily_ = AF_INET;
    this->connStr_ = NULL;
}

Packet::Packet(const Packet& old) {
    this->sip_ = old.sip_;
    this->sport_ = old.sport_;
    this->dip_ = old.dip_;
    this->dport_ = old.dport_;
    this->len_ = old.len_;
    this->direction_ = old.direction_;
    this->time_ = old.time_;
    this->saFamily_ = old.saFamily_;

    if (old.connStr_ == NULL)
        connStr_ = old.connStr_;
    else
        connStr_ = strdup(old.connStr_);
}

Packet::~Packet() {
    if (this->connStr_ != NULL)
        free(this->connStr_);
}

Packet * Packet::newInverted() {
    Direction dir = UNKNOWN;
    if (this->direction_ == OUTGOING)
        dir = INCOMING;
    else if (this->direction_ == INCOMING)
        dir = OUTGOING;

    return new Packet (dip_, dport_, sip_, sport_, len_, time_, dir);
}


/*
 * ===  FUNCTION  ======================================================================
 *         Name:  getNullIncomingPkt
 *  Description:  return a copy of given pkt with zero length
 * =====================================================================================
 */
Packet * Packet::getNullIncomingPkt(Packet * pkt) {
    Packet * ret;
    if (pkt->isOutgoing())
        ret = pkt->newInverted();
    else
        ret = new Packet(*pkt);

    ret->len_ = 0;
    return ret;
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  getNullOutgoingPkt
 *  Description:
 * =====================================================================================
 */
Packet * Packet::getNullOutgoingPkt(Packet * pkt) {
    Packet * ret;
    if (pkt->isOutgoing())
        ret = new Packet(*pkt);
    else
        ret = pkt->newInverted();

    ret->len_ = 0;
    return ret;
}


/*
 * ===  FUNCTION  ======================================================================
 *         Name:  getConnStr
 *  Description:
 * =====================================================================================
 */
char *Packet::getConnStr() {
    if (this->connStr_)
        goto out;

    connStr_ = (char *) malloc(CONNSTRLEN * sizeof(char));
    char localStr[50];
    char remoteStr[50];

    if (connStr_) {
        inet_ntop(saFamily_, &sip_, localStr, 49);
        inet_ntop(saFamily_, &dip_, remoteStr, 49);

        if (isOutgoing()) {
            snprintf(this->connStr_, CONNSTRLEN * sizeof(char),
              "%s:%d-%s:%d", localStr, sport_, remoteStr, dport_);
        } else {
            snprintf(this->connStr_, CONNSTRLEN * sizeof(char),
              "%s:%d-%s:%d", remoteStr, dport_, localStr, sport_);
        }
    }

out:
    return connStr_;
}

bool Packet::isOlderThan(timeval time) {
    return (time_.tv_sec <= time.tv_sec);
}

bool Packet::isOutgoing() {
    switch (this->direction_) {
        case OUTGOING:
            return true;
        case INCOMING:
            return false;
        case UNKNOWN:
            bool isLocal = false;
            if (this->saFamily_ == AF_INET)
                isLocal = NetworkMonitor::getNetworkMonitor()
                  ->contains(this->sip_.s_addr);
            if (isLocal)
                this->direction_ = OUTGOING;
            else
                this->direction_ = INCOMING;
            return isLocal;
    }
    return false;
}

bool Packet::match(Packet *packet) {
    // we neglect the comparison between the source ip and dest ip,
    // since the local host may have multiple interfaces and
    // packets of the same connection may be sent over different interfaces
    bool ret = (this->sport_ == packet->sport_)
        && (this->dport_ == packet->dport_)
        && (this->dip_.s_addr == packet->dip_.s_addr)
        && (this->sip_.s_addr == packet->sip_.s_addr);

    return ret;
}
