/*
 * =====================================================================================
 *
 *       Filename:  netmon.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/08/2014 02:04:24 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *   Organization:  
 *
 * =====================================================================================
 */

#ifndef __NETMON_H
#define __NETMON_H

#include <netinet/in.h>
#include <inttypes.h>
#include <vector>
#include <map>

class Interface;

class NetworkMonitor{
    public:
        void startMonitor();
        void addInterface(char *);
        bool contains(const in_addr_t &);

        static NetworkMonitor * getNetworkMonitor( ) {
            static NetworkMonitor monitor;
            return &monitor;
        }

    private:
        std::vector<Interface *> interfaces_;

        NetworkMonitor(); 
        ~NetworkMonitor(); 
};

#endif
