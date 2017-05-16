/*
 * =====================================================================================
 *
 *       Filename:  process.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/06/2014 01:37:37 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *   Organization:  
 *
 * =====================================================================================
 */
#ifndef __PROCESS_H
#define __PROCESS_H

#include <inttypes.h>
#include <sys/time.h>

#include <vector>
#include <map>

#define MAXSLOTKEPT 300 // resource usage of recent 300 slots will be kept

class ProcessManager;
class Connection;
class Packet;

class Process{
    friend class ProcessManager;
    public:
        Connection* findConnection(Packet *);
        void addConnection(Connection *);
        void updateSystemUsage();
        int updateNetworkSocket();

    private:
        int pid_;
        std::string name_;
        uint64_t startTime_;

        // resource usage
        uint64_t utime_;                //user time
        uint64_t stime_;                //kernel time
        uint64_t rssSize_;              //resident set size
        uint64_t vmSize_;               //virtual memory size
        uint64_t bytesRead_;             //total bytes read
        uint64_t bytesWritten_;          //total bytes written
        uint64_t bytesReadFromDisk_;     //bytes read from disk
        uint64_t bytesWrittenToDisk_;    //bytes written to disk
        uint64_t bytesSent_;            //bytes sent 
        uint64_t bytesRecv_;            //bytes recv'd

        // metrics read last time
        uint64_t lastUtime_;             
        uint64_t lastStime_;             
        //uint64_t lastRssSize_;         
        //uint64_t lastVmSize_;          
        uint64_t lastBytesRead_;         
        uint64_t lastBytesWritten_;      
        uint64_t lastBytesReadFromDisk_; 
        uint64_t lastBytesWrittenToDisk_;
        //uint64_t lastBytesSent_;        
        //uint64_t lastBytesRecv_;        



        std::vector<Connection *> connList_; //the connections of me

        static ProcessManager * procMgr_;

        Process(int pid, std::string pname);
        ~Process();

        //struct timeval lastSlotTime_;
        void countSystemUsage();
        void countIoUsage();
        void countNetworkUsage();
};

class ProcessManager{
    public: 
        void refreshMap();
        int refresh(std::map<int, std::string>& pidList, char * buf);
        Process* findProcess(uint64_t);
        bool containMap(uint64_t, Process*);
        inline void updateMap(uint64_t inode, Process* proc) {
            inodeToProcMap_[inode] = proc;
        }

        // sigleton
        static ProcessManager* getProcMgr () {
            static ProcessManager manager;
            return &manager;
        }

    private:
        struct timeval lastSlotTime_;
        uint64_t cpuTime_;
        uint64_t lastCpuTime_;
        double lastSlotLen_;

        std::vector<Process *> procList_;
        Process * unknownProc_;
        std::map<uint64_t, Process *> inodeToProcMap_;

        ProcessManager();
        ~ProcessManager();

        void updateProcessList(std::map<int, std::string>& pidList);
        void transferConnection(Process *);
        void countCpuUsage();
        void countSlotLen();
        void printProcessInfo();
        int printProcessInfoToBuf(char* buf);
};

#endif
