/*
 * =====================================================================================
 *
 *       Filename:  connection.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/08/2014 12:53:26 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *   Organization:  
 *
 * =====================================================================================
 */

#ifndef CONNECTION_H_
#define CONNECTION_H_

#include <vector>
#include <map>
#include <string>

#include <inttypes.h>

class Packet;
class Process;
class NetworkMonitor;

class Connection{
    public:
        Connection(Packet *);
        ~Connection();

        bool includePkt (Packet *);
        void addPkt (Packet *);
        void closeCurrentSlot ();
        void sum (uint64_t * sent, uint64_t * recv, size_t numSlot = 1);
        bool isAlive();

        inline Packet* getRefPkt() {
            return refPkt_;
        }

        inline uint32_t getTotalPktSent () {
            return totalPktSent_;
        }

        inline uint32_t getTotalPktRecv () {
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

        void refreshMap();

        Connection * findConnection (Packet *);

        inline void addConnection (Connection * conn) {
            connList_.push_back(conn);
        }

        void addInterface(char *);

        uint64_t findInode(Connection *);

        static ConnectionManager * getConnMgr() {
            static ConnectionManager manager;
            return &manager;
        }

    private:
        std::vector<Connection*> connList_; 
        std::vector<char *> ifaceName_;
        //NetworkMonitor* monitor_;
        std::map<std::string, uint64_t> connToInodeMap_;

        void updateMap(char *);
        ConnectionManager();
        //ConnectionManager(NetworkMonitor *);
        ~ConnectionManager();
};
#endif
