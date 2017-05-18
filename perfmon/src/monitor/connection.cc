// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#include <assert.h>
#include <cstring>
#include <vector>
#include <mutex>
#include <atomic>
#include <iostream>
#include <map>

#include "monitor/connection.h"
#include "monitor/netmon.h"
#include "monitor/packet.h"
#include "monitor/interface.h"
#include "monitor/config.h"

#define HASHKEYSIZE 92
#define MAXSLOTKEPT 300  // five minutes?

extern std::atomic_ulong curTime_;
extern std::mutex globalMutex_;

Connection::Connection(Packet *pkt) {
    this->totalPktRecv_ = 0;
    this->totalPktSent_ = 0;

    owner_ = NULL;

    if (pkt->isOutgoing()) {
        refPkt_ = new Packet(*pkt);
    } else {
        refPkt_ = pkt->newInverted();
    }

    nullIncomingPkt_ = Packet::getNullIncomingPkt(pkt);
    nullOutgoingPkt_ = Packet::getNullOutgoingPkt(pkt);
    lastinComingPkt_ = new Packet(*nullIncomingPkt_);
    lastOutgoingPkt_ = new Packet(*nullOutgoingPkt_);

    addPkt(pkt);
}

Connection::~Connection() {
    delete refPkt_;
    delete nullIncomingPkt_;
    delete nullOutgoingPkt_;
    if (lastinComingPkt_)
        delete lastinComingPkt_;
    if (lastOutgoingPkt_)
        delete lastOutgoingPkt_;
    std::vector<Packet *>::iterator it;
    for (it = sentPkts_.begin(); it != sentPkts_.end(); it++)
        delete (*it);
    for (it = recvPkts_.begin(); it != recvPkts_.end(); it++)
        delete (*it);

    sentPkts_.clear();
    recvPkts_.clear();
}


/*
 * ===  FUNCTION  ======================================================================
 *         Name:  addPkt
 *  Description:  add the length of given packet to the stat of current slot
 * =====================================================================================
 */
void Connection::addPkt(Packet *pkt) {
    // we need to lock the lastPkt_, as it may be
    // modified by the display thread

    this->lastPktTime_ = curTime_.load();

    assert(lastOutgoingPkt_);
    assert(lastinComingPkt_);

    if (pkt->isOutgoing()) {
#ifdef PERF_DEBUG
        // std::cout << "capture an outgoing packet with length "
        // << pkt->getLength() << "\n";
#endif
        lastOutgoingPkt_->addLength(pkt->getLength());
        totalPktSent_ += pkt->getLength();
    } else {
#ifdef PERF_DEBUG
        // std::cout << "capture an incoming packet with length "
        // << pkt->getLength() << "\n";
#endif
        lastinComingPkt_->addLength(pkt->getLength());
        totalPktRecv_ += pkt->getLength();
    }


    // if (pkt->isOutgoing()) {
    //    if (lastOutgoingPkt_ == NULL)
    //        lastOutgoingPkt_ = new Packet(*pkt);
    //    else
    //        lastOutgoingPkt_->addLength(pkt->getLength());
    //    totalPktSent_ += pkt->getLength();
    // } else {
    //    if (lastinComingPkt_ == NULL)
    //        lastinComingPkt_ = new Packet(*pkt);
    //    else
    //        lastinComingPkt_->addLength(pkt->getLength());
    //    totalPktSent_ += pkt->getLength();
    // }
}



/*
 * ===  FUNCTION  ======================================================================
 *         Name:  includePkt
 *  Description:  check if the given packet belongs to this Connection
 * =====================================================================================
 */
bool Connection::includePkt(Packet * pkt) {
    if (refPkt_->match(pkt)) {
        return true;
    }

    Packet* invertedPkt = pkt->newInverted();

    if (refPkt_->match(invertedPkt)) {
        delete invertedPkt;
        return true;
    }

    delete invertedPkt;
    return false;
}


/*
 * ===  FUNCTION  ======================================================================
 *         Name:  closeCurrentSlot
 *  Description:  close the current slot and initialize the next one
 * =====================================================================================
 */
    void
Connection::closeCurrentSlot() {
    this->sentPkts_.push_back(lastOutgoingPkt_);
    this->recvPkts_.push_back(lastinComingPkt_);

    // delete the packets that were sent/recv'd long time ago
    while (this->sentPkts_.size() > MAXSLOTKEPT) {
        delete (*(this->sentPkts_.begin()));
        delete (*(this->recvPkts_.begin()));
        this->sentPkts_.erase(this->sentPkts_.begin());
        this->recvPkts_.erase(this->recvPkts_.begin());
    }

    lastOutgoingPkt_ = new Packet(*nullOutgoingPkt_);
    lastinComingPkt_ = new Packet(*nullIncomingPkt_);
}   /* -----  end of function closeCurrentSlot  ----- */


/*
 * ===  FUNCTION  ======================================================================
 *         Name:  sum
 *  Description:  count the number of bytes sent and recv'd during the given
 *                slots
 * =====================================================================================
 */
void
Connection::sum(uint64_t *sent, uint64_t *recv, size_t numSlot) {
    *sent = *recv = 0;

    int idx = (numSlot > sentPkts_.size()) ? 0 : sentPkts_.size() - numSlot;

#ifdef PERF_DEBUG
    std::cout << "size of pkt vector: " << sentPkts_.size() << std::endl;
#endif

    for (int i = sentPkts_.size() - 1; i >= idx; i--) {
        *sent += sentPkts_[i]->getLength();
        *recv += recvPkts_[i]->getLength();
#ifdef PERF_DEBUG
        std::cout << "sent = " << *sent;
        std::cout << " recv = " << *recv << std::endl;
#endif
    }
}


/*
 * ===  FUNCTION  ======================================================================
 *         Name:  isAlive
 *  Description:  check if this connection is still alive or not
 *                1) it has recv'd at least one packet during the last MAXSLOTKEPT
 *                   slots
 *                2) its owner still exists
 * =====================================================================================
 */
bool
Connection::isAlive() {
    return ((owner_) && (lastPktTime_ + MAXSLOTKEPT >= curTime_.load()));
}


/*
 * ===  FUNCTION  ======================================================================
 *         Name:  ConnectionManager
 *  Description:
 * =====================================================================================
 */
ConnectionManager::ConnectionManager() {}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  ~ConnectionManager
 *  Description:
 * =====================================================================================
 */
ConnectionManager::~ConnectionManager() {
#ifdef PERF_DEBUG
    std::cout << "cleaning up for ConnectionManager\n";
#endif

    this->connToInodeMap_.clear();
    unsigned long i;

    for (i = 0; i < connList_.size(); i++) {
        delete(connList_[i]);
    }

    for (i = 0; i < ifaceName_.size(); i++) {
        free(ifaceName_[i]);
    }

    connList_.clear();
    ifaceName_.clear();
}


/*
 * ===  FUNCTION  ======================================================================
 *         Name:  addInterface
 *  Description:
 * =====================================================================================
 */
    void
ConnectionManager::addInterface(char * iface) {
    this->ifaceName_.push_back(strdup(iface));
}


/*
 * ===  FUNCTION  ======================================================================
 *         Name:  refreshMap
 *  Description:  refresh conn-to-inode map
 * =====================================================================================
 */
void ConnectionManager::refreshMap() {
    // refresh conn-to-inode map

    // 1. not maintain an "inode" variable in class "Connection"
    //    since connection instances are distributed over multiple
    //    processes, and it's hard to perform lookup against them
    // 2. Each process can safely delete its own Connections, without
    //    affecting this global map, as the key of map is the conn string

    FILE* tcpInfo = fopen("/proc/net/tcp", "r");
    char buffer[8192];

    if (tcpInfo == NULL) {
        std::cout << "cannot open /proc/net/tcp\n";
        return;
    }

    // skip the first line
    if (fgets(buffer, sizeof(buffer), tcpInfo) != buffer) {
      std::cout << "cannot read file tcpInfo\n";
    }

    while (!feof(tcpInfo)) {
        if (fgets(buffer, sizeof(buffer), tcpInfo))
            updateMap(buffer);
    }

    fclose(tcpInfo);
}


/*
 * ===  FUNCTION  ======================================================================
 *         Name:  updateMap
 *  Description:  update an entry of connection-to-inode map from given string
 * =====================================================================================
 */
void ConnectionManager::updateMap(char * buffer) {
    short int sa_family;
    struct in6_addr result_addr_local;
    struct in6_addr result_addr_remote;

    char rem_addr[128], local_addr[128];
    int local_port, rem_port;

    unsigned long inode;

    static const char format[] = "%*d: %64[0-9A-Fa-f]:%X %64[0-9A-Fa-f]:%X %*X"
                                 "%*X:%*X %*X:%*X %*X %*d %*d %ld %*512s\n";
    int matches = sscanf(buffer, format,
    //  "%*d: %64[0-9A-Fa-f]:%X %64
    //  [0-9A-Fa-f]:%X %*X %*X:%*X %*X:%*X %*X %
    //  *d %*d %ld %*512s\n",
      local_addr, &local_port, rem_addr, &rem_port, &inode);

    if (matches != 5) {
        fprintf(stderr, "Unexpected buffer: '%s'\n", buffer);
        exit(0);
    }

    if (inode == 0) {
        /* connection is in TIME_WAIT state. We rely on
         * the old data still in the table. */
        return;
    }

    if (strlen(local_addr) > 8) {
        /* this is an IPv6-style row */
    } else {
        /* this is an IPv4-style row */
        sscanf(local_addr, "%X", (unsigned int *) &result_addr_local);
        sscanf(rem_addr, "%X",   (unsigned int *) &result_addr_remote);
        sa_family = AF_INET;

        char * hashkey = (char *) malloc (HASHKEYSIZE * sizeof(char));
        char * local_string = (char*) malloc(50);
        char * remote_string = (char*) malloc(50);
        inet_ntop(sa_family, &result_addr_local,  local_string,  49);
        inet_ntop(sa_family, &result_addr_remote, remote_string, 49);

        snprintf(hashkey, HASHKEYSIZE * sizeof(char),
          "%s:%d-%s:%d", local_string, local_port, remote_string, rem_port);
        free(local_string);

        this->connToInodeMap_[hashkey] = inode;
#ifdef PERF_DEBUG
        std::cout << "KEY: " << hashkey << " inode: " << inode << std::endl;
#endif

        /* workaround: sometimes, when a connection is actually from 172.16.3.1 to
         * 172.16.3.3, packages arrive from 195.169.216.157 to 172.16.3.3, where
         * 172.16.3.1 and 195.169.216.157 are the local addresses of different
         * interfaces */
        for (std::vector<char *>::iterator it = ifaceName_.begin();
          it != ifaceName_.end(); it++) {
            /* TODO maybe only add the ones with the same sa_family */
            snprintf(hashkey, HASHKEYSIZE * sizeof(char),
              "%s:%d-%s:%d", (*it), local_port, remote_string, rem_port);
            connToInodeMap_[hashkey] = inode;
#ifdef PERF_DEBUG
            std::cout << hashkey << std::endl;
#endif
        }
        free(hashkey);
        free(remote_string);
    }
}


/*
 * ===  FUNCTION  ======================================================================
 *         Name:  findConnection
 *  Description:  return the connection containing the given pkt
 * =====================================================================================
 */
Connection *ConnectionManager::findConnection(Packet * pkt) {
    Connection * conn = NULL;

    // delete those connections that have not recv'd pkts for a long time
    // or have a dead owner as we go over the connection list

    for (std::vector<Connection *>::iterator it = connList_.begin();
            it != connList_.end();) {
        if (!(*it)->isAlive()) {
            delete (*it);
            it = connList_.erase(it);
            continue;
        }

        if ((*it)->includePkt(pkt)) {
            conn = (*it);
            break;
        }

        it++;
    }

    return conn;
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  findInode
 *  Description:  return the inode of the given connection
 * =====================================================================================
 */
uint64_t ConnectionManager::findInode(Connection * conn) {
    return connToInodeMap_[conn->getRefPkt()->getConnStr()];
}
