#ifndef INCLUDE_DAEMON_H
#define INCLUDE_DAEMON_H

#include <cstdio>
#include "utils/sock.h"
#include "utils/protobuf.h"
#include "perfmon.h"

class PerfmonDaemon{
    public:
        //constant
        const static int BUF_LEN = PerformanceMonitor::BUF_LEN;
        const static int UNIT = sizeof(struct ProcInfo);
        //http server
        int http_port;
        ProtoBuffer buffer;
    private:
        //socket
        int sock_port;
        SocketServer socket;
        char buf[BUF_LEN];
    
    public:
        PerfmonDaemon(int monitor_port, int ui_port);
        ~PerfmonDaemon();
        void start();
    private:
        void processMessage(const char* hostname, int len);
};

#endif
