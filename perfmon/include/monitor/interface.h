/*
 * =====================================================================================
 *
 *       Filename:  interface.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/09/2014 02:07:46 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *   Organization:  
 *
 * =====================================================================================
 */

#ifndef __INTERFACE_H
#define __INTERFACE_H

#include <netinet/in.h>
#include <inttypes.h>

class PacketHandler;

class Interface{
    public:
        Interface ( ) {}
        ~Interface();
        bool contains(const in_addr_t &);
        int initInterface(const char *dev);
        char * getAddr();
        int processNextPkt();


    private:
        char *dev_;
        uint16_t saFamily_;
        in_addr_t addr_;
        char * str_;
        PacketHandler *handler_;
};
#endif

