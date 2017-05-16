/*
 * =====================================================================================
 *
 *       Filename:  perfmon.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/16/2014 08:05:02 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  CAI Qingchao (), qingchaochoi@gmail.com
 *   Organization:  
 *
 * =====================================================================================
 */

#ifndef __PERFMON_H
#define __PERFMON_H

#include <cstdio>
#include "utils/sock.h"
#include "utils/proto.h"

#include <map>

class NetworkMonitor;
class ProcessManager;

class PerformanceMonitor{
    public:
        PerformanceMonitor(const char * const hostname, int port,  char* const dir, int numNic = 0, char** nicList = NULL);
        ~PerformanceMonitor();
        void start();
    private:
        char* dir_;
        void refreshProcess();

    public:
        //constant
        const static int MAX_PROC = 50;
        const static int BUF_LEN = (MAX_PROC+1)*(sizeof(struct ProcInfo));
    private:
        //socket
        SocketClient socket;
        char buf[BUF_LEN];
        //monitor
        char pidFile[BUF_LEN];

        map<int, string> pidList_;
};

#endif
