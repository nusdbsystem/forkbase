/*
 * =====================================================================================
 *
 *       Filename:  netmon.c
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/12/2014 05:22:18 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *   Organization:  
 *
 * =====================================================================================
 */
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <vector>
#include <atomic>
#include <mutex>

#include "monitor/netmon.h"
#include "monitor/connection.h"
#include "monitor/handler.h"
#include "monitor/interface.h"
#include "monitor/config.h"

std::mutex globalMutex_;

extern volatile sig_atomic_t stopSign_;

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  NetworkMonitor
 *  Description:  
 * =====================================================================================
 */
NetworkMonitor::NetworkMonitor ( ) { 
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  ~NetworkMonitor
 *  Description:  
 * =====================================================================================
 */
NetworkMonitor::~NetworkMonitor ( )
{
#ifdef PERF_DEBUG
    std::cout << "cleaning up for NetworkMonitor\n";
#endif

    for (std::vector<Interface *>::iterator it = interfaces_.begin();
                it != interfaces_.end(); it++) {
        delete(*it);
    }

    interfaces_.clear();

    //delete connMgr_;
}

    bool
NetworkMonitor::contains (const in_addr_t &addr) {
    for (std::vector<Interface *>::iterator it = interfaces_.begin();
            it != interfaces_.end(); it++) {
        if ((*it)->contains(addr))
            return true;
    }

    return false;
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  addInterface
 *  Description:  add an network interface for monitor
 * =====================================================================================
 */
void NetworkMonitor::addInterface (char * dev) {

    Interface* iface = new Interface();

    if ( iface->initInterface(dev) < 0 )
    {
        std::cout << "fail to initialize interface " << dev << std::endl;
        delete iface;
        return;
    }

    this->interfaces_.push_back(iface);
    ConnectionManager::getConnMgr()->addInterface(iface->getAddr());
}


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  startMonitor
 *  Description:  
 * =====================================================================================
 */
void NetworkMonitor::startMonitor () {
    int ret;
    bool pktRead;
    while (!stopSign_) {
        pktRead = false;
        for (std::vector<Interface *>::iterator it = interfaces_.begin();
                it != interfaces_.end(); it++) {
            ret = (*it)->processNextPkt();
            if (ret == -1 || ret == -2) {
                std::cerr << "error dispatching \n";
            } else {
                if (!pktRead && ret > 0)
                    pktRead = true;
            }
        }

        if (!pktRead)
            usleep(100);
    }
}

