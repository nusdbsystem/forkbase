/*
 * =====================================================================================
 *
 *       Filename:  handler.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/08/2014 08:19:51 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *   Organization:  
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <string>
#include <iostream>
#include <vector>
#include <mutex>

#include <net/ethernet.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <netinet/udp.h>

#include "monitor/handler.h"
#include "monitor/packet.h"
#include "monitor/connection.h"
#include "monitor/process.h"
#include "monitor/config.h"

extern "C" {
#include <pcap.h>
#include <pcap/sll.h>
}

extern std::mutex globalMutex_;

// parser
void parseTcpPkt (PacketHandler *, const PacketHeader *, const u_char *);
void parseIpPkt (PacketHandler *, const PacketHeader *, const u_char *);
void parseSllPkt (PacketHandler *, const PacketHeader *, const u_char *);
void parseEthernetPkt (PacketHandler *, const PacketHeader *, const u_char *);

//int processTcpPkt (u_char *, const PacketHeader *, const u_char *);
//int processIpPkt (u_char *, const PacketHeader *, const u_char *);
//int processUdpPkt (u_char *, const PacketHeader *, const u_char *);

void pcapCallback (u_char *, const struct pcap_pkthdr *, const u_char *);


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  pcapCallback
 *  Description:  
 * =====================================================================================
 */
void pcapCallback (u_char * u_handler, const struct pcap_pkthdr * header, const u_char * packet){
    PacketHandler * handler = (PacketHandler *) u_handler;
    handler->startParseAndProcess(header, packet);
}


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  parseTcpPkt
 *  Description:  
 * =====================================================================================
 */
void parseTcpPkt (PacketHandler * handler, const PacketHeader * header, const u_char * packet) {

    handler->processTcpPkt (header, packet);
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  parseIpPkt
 *  Description:  
 * =====================================================================================
 */
void parseIpPkt (PacketHandler * handler, const PacketHeader * header, const u_char * packet) {
    const struct ip * ip = (struct ip *) packet;
    u_char * payload = (u_char *) packet + sizeof (struct ip);

    if (handler->processIpPkt (header, packet)) {
        return; 
    }

    // we only care tcp packet
    if (ip->ip_p == 6) {
        parseTcpPkt (handler, header, payload);
    }
}


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  parseEthernetPkt
 *  Description:  parse the data link layer packet
 * =====================================================================================
 */
void parseEthernetPkt (PacketHandler * handler, const PacketHeader * header, const u_char * packet) {
    const struct ether_header *ethHeader = (struct ether_header *)packet;
    u_char * payload = (u_char *)packet + sizeof (struct ether_header);

    switch (ethHeader->ether_type) 
    {
        // we only process IPv4 packet
        case (0x0008):
            parseIpPkt (handler, header, payload);
        default:
            break;
    }
}


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  parseSllPkt
 *  Description:  parse linux-cooked packet (extracting from "ANY" device)
 * =====================================================================================
 */
void parseSllPkt (PacketHandler * handler, PacketHeader * header, const u_char * packet) {
    const struct sll_header * sll = (struct sll_header *) packet;
    u_char * payload = (u_char *) packet + sizeof (struct sll_header);

    switch (sll->sll_protocol) 
    {
        // we only process IPv4 packet
        case (0x0008):
            parseIpPkt (handler, header, payload);
        default:
            break;
    }
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  PacketHandler
 *  Description:  
 * =====================================================================================
 */
PacketHandler::PacketHandler (char *dev) {
    dev_ = strdup(dev);
    userDataSize_ = sizeof (ConnAddress);
    userData_ = new ConnAddress;
    //userData_ = (ConnAddress *) malloc (userDataSize_);
};


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  ~PacketHandler
 *  Description:  
 * =====================================================================================
 */
PacketHandler::~PacketHandler () {
    pcap_close(this->pcapHandler_);
    free(dev_);
    delete userData_;
};


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  setConnMgr
 *  Description:  
 * =====================================================================================
 */
//void PacketHandler::setConnMgr(ConnectionManager * mgr)
//{
//    connMgr_ = mgr;
//}


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  startParseAndProcess
 *  Description:  
 * =====================================================================================
 */
void PacketHandler::startParseAndProcess (const struct pcap_pkthdr * header, const u_char * packet) {
    PacketHeader * hdr = (PacketHeader *) header;
    this->pktParser_ (this, hdr, packet);
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  processIpPkt
 *  Description:  
 * =====================================================================================
 */
bool PacketHandler::processIpPkt (const PacketHeader * header, const u_char * packet) {
    struct ip * ip = (struct ip *) packet;
    this->userData_->sa_family = AF_INET;
    this->userData_->src = ip->ip_src;
    this->userData_->dst = ip->ip_dst;

    // we have not done yet
    return false;
}


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  processTcpPkt
 *  Description:  
 * =====================================================================================
 */
bool PacketHandler::processTcpPkt (const PacketHeader * header, const u_char * packet) {
    struct tcphdr * tcp = (struct tcphdr *)packet;

    Packet *pkt = new Packet (userData_->src, ntohs(tcp->source), userData_->dst, ntohs(tcp->dest), header->len, header->ts);

    this->process(pkt);

    //we are done
    delete pkt;
    return true;

}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  process
 *  Description:  process the network packet
 * =====================================================================================
 */
void PacketHandler::process (Packet* pkt)
{
    // synchorize 
    std::unique_lock<std::mutex> lock(globalMutex_);

    ConnectionManager* connMgr = ConnectionManager::getConnMgr();
    ProcessManager* procMgr = ProcessManager::getProcMgr();

    // first find if this packet belongs to an existing connection
    Connection *conn = connMgr->findConnection(pkt);

    if (conn) {
        conn->addPkt(pkt);
        return;
    }

    //if not, set up a new connection with owner of unkonw process
    conn = new Connection(pkt);

#ifdef PERF_DEBUG
    std::cout << conn->getRefPkt()->getConnStr() << std::endl;
#endif

    // refresh the conn-to-inode map
    connMgr->refreshMap();

    uint64_t inode = connMgr->findInode(conn);

    Process * proc = procMgr->findProcess(inode);

    proc->addConnection(conn);
    
    connMgr->addConnection(conn);
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  openDevLive
 *  Description:  
 * =====================================================================================
 */
int PacketHandler::openDevLive () {
    char ebuf[PCAP_ERRBUF_SIZE];
    pcap_t * temp = pcap_open_live (dev_, BUFSIZ, 0, 100, ebuf);

    if (temp) {
        return (fillHandler (temp));
    } else {
        return -1;
    }
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  fillHandler
 *  Description:  
 * =====================================================================================
 */
int PacketHandler::fillHandler (pcap_t *handler) {
   this->pcapHandler_ = handler;
   this->linkType_ = pcap_datalink (this->pcapHandler_);

   switch (this->linkType_) {
       case (DLT_EN10MB):
           this->pktParser_ = parseEthernetPkt;
           break;
       case (DLT_LINUX_SLL):
           this->pktParser_ = parseSllPkt;
       case (DLT_RAW):
       case(DLT_NULL):
           this->pktParser_ = parseIpPkt;
           break;
       default:
           std::cerr << "Unknown link type: " << this->linkType_ << std::endl;
           break;
   }


   return 0;
}


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  sentNonBlock
 *  Description:  set the nonblock mode. an attempt to read a packet will
 *                immediately return if no packets in the buffer
 * =====================================================================================
 */
int PacketHandler::setNonBlock () {
    char ebuf[PCAP_ERRBUF_SIZE];
    return pcap_setnonblock(this->pcapHandler_, 1, ebuf);
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  dispatch
 *  Description:  
 * =====================================================================================
 */
int PacketHandler::dispatch (int count) {
    return pcap_dispatch (this->pcapHandler_, count, pcapCallback, (u_char *)this);
}



