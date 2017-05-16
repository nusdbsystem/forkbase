/*
 * =====================================================================================
 *
 *       Filename:  perfmon.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/16/2014 08:21:07 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  CAI Qingchao (), qingchaochoi@gmail.com
 *   Organization:  
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <dirent.h>
#include <cstdio>
#include <cstring>
#include <thread>
#include <iostream>
#include <atomic>
#include <sys/time.h>

#include "perfmon.h"
#include "monitor/netmon.h"
#include "monitor/process.h"
#include "monitor/handler.h"
#include "monitor/config.h"
#include "utils/regist.h"
#include "utils/config.h"

volatile sig_atomic_t stopSign_ = 0;

std::atomic_ulong curTime_(0);

void work();

extern string PERFMON_REG_PATH;

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  signal_handler
 *  Description:  handler of SIGINT signal
 * =====================================================================================
 */
void signal_handler ( int signal )
{
    stopSign_ = 1;
}		/* -----  end of function signal_handler  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  PerformanceMonitor
 *  Description:  Constructor
 * =====================================================================================
 */
PerformanceMonitor::PerformanceMonitor(const char* const hostname, int port, char* const dir, int numNic, char** nicList)
{
    if (dir[strlen(dir) - 1] == '/')
        dir[strlen(dir) - 1] = '\0';

    //init socket connection
    while (!socket.cli_init(hostname, port)){
      fprintf(stderr, "cannot connect to %s:%d, try later... \n", hostname, port);
      usleep(2000000);
    }

    //init pid dir
    this->dir_ = strdup(dir);
    char dev[10] = "eth0";

    //init network monitor
    NetworkMonitor* monitor = NetworkMonitor::getNetworkMonitor(); 

    // if there is no interfaces given, we use device eth0 as the device
    // for network monitoring
    if (numNic == 0)
    {
        monitor->addInterface(dev);
    } else {
        for (int i = 0; i < numNic; i++) {
            monitor->addInterface(nicList[i]);
        }
    }
}


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  ~PerformanceMonitor
 *  Description:  
 * =====================================================================================
 */
PerformanceMonitor::~PerformanceMonitor () {
    pidList_.clear();
    free(dir_);
    socket.cli_close();
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  start
 *  Description:  start monitoring
 * =====================================================================================
 */
void PerformanceMonitor::start (){
    // initialize one individual thread for network monitor
    std::thread netThread(work);

    // run once every 1 second
    //long sleep_total = 1000000;
    //timeval tim;
    //long sleep_remain;

    // periodically refresh process, once per 1 second
    while (!stopSign_) {
        //get start time
        //gettimeofday(&tim, NULL);
        //sleep_remain = tim.tv_sec * 1000000 +tim.tv_usec;
        //printf("%ld\n", sleep_remain);

        refreshProcess();
        
        //get end time
        //gettimeofday(&tim, NULL);
        //sleep_remain -= tim.tv_sec * 1000000 +tim.tv_usec;
        //printf("%ld\n", sleep_remain);
        //sleep_remain += sleep_total;
        //printf("%ld\n", sleep_remain);

        //printf("usleep = %ld\n", sleep_remain);
        usleep(1000000);
        //if (sleep_remain > 0) usleep(sleep_remain);
    }

    netThread.join(); // wait for the thread to complete
}		/* -----  end of function start  ----- */

void PerformanceMonitor::refreshProcess(){
    //update pid directory
    string cmd = string("./script/ps.sh ");
    //printf("cmd = %s\n", cmd.c_str());
    int stat = system(cmd.c_str());
    if (stat != 0){
      fprintf(stderr, "error when running script: %s\n", cmd.c_str());
    }

    // read each file in the pid directory, and get
    // the process id for monitoring
    DIR* dir = opendir(this->dir_);
    if (!dir)
    {
        std::cout << "cannot open " << this->dir_ << "\n";
        return;
    }

    curTime_.operator++();

    dirent* entry;
    int maxProcLength = 10;
    char pid[maxProcLength+1];
    FILE* file;

    pidList_.clear();

    while ((entry = readdir(dir))) {
        if (entry->d_type != DT_REG)
            continue;
        sprintf (pidFile, "%s/%s", this->dir_, entry->d_name);

        file = fopen(pidFile, "r");
        if (file == NULL) {
            std::cout << "cannot open " << pidFile << std::endl;
            continue;
        }
        
        while (fgets(pid, maxProcLength, file)) {
            std::string s(entry->d_name);
            pidList_[atoi(pid)] = s;
        }

        fclose(file);
    }

    //print process info into buf
    int len = (ProcessManager::getProcMgr()->refresh(pidList_, buf));

    //send procs info to server via socket
    if (len > 0){
      socket.send_data(buf, len);
    }

    closedir(dir);
}


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  work
 *  Description:  the function executed by the thread
 * =====================================================================================
 */
void work() {
    NetworkMonitor::getNetworkMonitor()->startMonitor();
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  main
 *  Description:  
 * =====================================================================================
 */
int main ( int argc, char *argv[] )
{
//    if (argc < 4) {
//        std::cerr << "%s usage: [hostname] [port] [pids dir] ( [dev1] [dev2] ... )\n";
//        return EXIT_SUCCESS;
//    }
//    if (argc < 3) {
//        std::cerr << "%s usage: [hostname] [port]\n";
//        return EXIT_SUCCESS;
//    }
    
    Config conf;
    conf.loadConfigFile("conf/perfmon.cfg");
    
    string sock_host = conf.get("sock_host");
    string sock_port = conf.get("sock_port");
    if (sock_host == "" || sock_port == ""){
      std::cerr << "please set sock_host and sock_port in configure file\n";
      return EXIT_SUCCESS;      
    }
    
    string pid_dir = conf.get("pid_dir");
    printf("pid dir = %s\n", pid_dir.c_str());
    if (pid_dir != "") {
        setRegistPath(pid_dir.c_str());
        printf("xxxx");
    }
    
    char* perf_path = new char[PERFMON_REG_PATH.length()+1];
    strcpy(perf_path, PERFMON_REG_PATH.c_str());
    printf("pid dir = %s\n", PERFMON_REG_PATH.c_str());
   
    registInPerfmon("perf_monitor");
    PerformanceMonitor * monitor = new PerformanceMonitor(sock_host.c_str(), atoi(sock_port.c_str()), perf_path);  
    signal (SIGINT, &signal_handler);

    monitor->start();

    delete monitor;
    return EXIT_SUCCESS;
}				/* ----------  end of function main  ---------- */
