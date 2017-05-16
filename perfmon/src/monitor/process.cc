/*
 * =====================================================================================
 *
 *       Filename:  process.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/11/2014 11:00:33 AM
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
#include <dirent.h>
#include <sys/stat.h>
#include <assert.h>
#include <cstring>
#include <string>
#include <iostream>
#include <iomanip>
#include <cstdio>
#include <sys/time.h>
#include <atomic>

#include <algorithm>
#include <mutex>

#include "monitor/process.h"
#include "monitor/connection.h"
#include "monitor/config.h"
#include "utils/proto.h"

extern std::atomic_ulong curTime_;
extern std::mutex globalMutex_;
ProcessManager* Process::procMgr_ = NULL;

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  Process
 *  Description:  constructor
 * =====================================================================================
 */
Process::Process (int pid, std::string pname): pid_(pid), name_(pname){
    startTime_ = curTime_.load();
}		/* -----  end of function Process  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  ~Process
 *  Description:  destructor
 * =====================================================================================
 */
Process::~Process() {
    // free the connections
    for (std::vector<Connection *>::iterator it = connList_.begin();
            it != connList_.begin(); it++) {
        // set the owner of my connections to null to indicate their
        // unaliveness so that there can be deleted by ConnectionManager
        (*it)->setOwner(NULL);
    }

    connList_.clear();
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  updateSystemUsage
 *  Description:  update the system resource usage  
 * =====================================================================================
 */
    void
Process::updateSystemUsage (  )
{
    this->countSystemUsage();
    this->countIoUsage();
    this->countNetworkUsage();
}		/* -----  end of function updateSystemUsage  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  countSystemUsage
 *  Description:  count the cpu and memory usage of current process during last
 *                time peirod
 * =====================================================================================
 */
void 
Process::countSystemUsage () {

    char statFile[20];
    sprintf(statFile, "/proc/%d/stat", this->pid_);
    FILE * file = fopen(statFile, "r");

    if (file == NULL) {
        std::cerr << "unable to open file: " << statFile << "\n";
        return;
    }

    char stat[4096];

    // only one line in this file
    // this line 52 fields, seperated by space, and we are interested
    // in the following fields:
    // pid (1), utime (14), stime (15), vsize (23), rss (24)
    if (fgets(stat, 4096, file) != stat){
      std::cout << "cannot read file stat\n";
    }

    std::string token(stat);
    size_t pos = 0;

    char delimiter = ' ';

    pos = 0;
    for (int i = 1; i <= 24; i++)
    {
        token = token.substr(pos + 1);

        if (14 == i) 
        {
            this->utime_ = std::stoull(token) - lastUtime_;
            lastUtime_ = std::stoull(token);
        }
        else if (15 == i) 
        {
            this->stime_ = std::stoull(token) - lastStime_;
            lastStime_ = std::stoull(token);
        }
        else if (23 == i) 
        {
            this->vmSize_ = std::stoull(token);
        }
        else if (24 == i) 
        {
            // it is in page unit
            this->rssSize_ = (std::stoull(token)) << 12;
        }

        pos = token.find(delimiter);
    }

    fclose(file);
}


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  countIoUsage
 *  Description:  count IO throughput during last time period
 * =====================================================================================
 */
void
Process::countIoUsage () {

    char ioFile[20];
    sprintf(ioFile, "/proc/%d/io", this->pid_);
    FILE * file = fopen(ioFile, "r");

    if (file == NULL) {
        std::cerr << "unable to open file: " << ioFile << "\n";
        return;
    }

    char stat[1024];

    // there are seven lines in this file
    // we are interested in the following lines:
    // rchar (1), wchar (2), read_bytes (5), write_bytes (6)

    char delimiter = ' ';
    size_t pos;
    std::string token;
    for (int i = 1; i <= 6; i++)
    {
        if (fgets(stat, 1024, file) != stat){
          std::cout << "cannot read file stat\n";
        }
        std::string str(stat);

        pos = str.find(delimiter);
        token = str.substr(pos+1);

        if (1 == i)
        {
            this->bytesRead_ = stoull(token) - lastBytesRead_;
            lastBytesRead_ = stoull(token);
        }
        else if (2 == i) {
            this->bytesWritten_ = stoull(token) - lastBytesWritten_;
            lastBytesWritten_ = stoull(token);
        }
        else if (5 == i) {
            this->bytesReadFromDisk_ = stoull(token) - lastBytesReadFromDisk_;
            lastBytesReadFromDisk_ = stoull(token);
        }
        else if (6 == i) {
            this->bytesWrittenToDisk_ = stoull(token) - lastBytesWrittenToDisk_;
            lastBytesWrittenToDisk_ = stoull(token);
        }
    }

    fclose(file);
}


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  updateNetworkUsage
 *  Description:  
 * =====================================================================================
 */
    void
Process::countNetworkUsage (  )
{
    this->bytesSent_ = 0;
    this->bytesRecv_ = 0;
    for (size_t i = 0; i < connList_.size(); i++) {
        connList_[i]->closeCurrentSlot();
        connList_[i]->sum(&this->bytesSent_, &this->bytesRecv_);
    }
}		/* -----  end of function updateNetworkUsage  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  addConnection
 *  Description:  add a connection to local connection list
 * =====================================================================================
 */
    void
Process::addConnection (Connection * conn)
{
    this->connList_.push_back(conn);
    conn->setOwner(this);
}		/* -----  end of function addConnection  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  updateNetworkSocket
 *  Description:  keep up-to-date the inodes of network sockets I opened
 * =====================================================================================
 */
int Process::updateNetworkSocket() {
    int dlen = 20;
    int flen;
    int llen = 100;
    char dirname[100];
    char linkname[100];
    char filename[100];

    snprintf(dirname, dlen, "/proc/%d/fd", pid_);

    DIR* dir = opendir(dirname);

    if (!dir) {
        std::cout << "couldn't open dir: " << dirname << "\n";
        // this process no longer alive, may we need to delete it?
        return -1;
    }

    dirent* entry;
    while ((entry = readdir(dir))) {
        if (entry->d_type != DT_LNK)
            continue;

        flen = dlen + strlen(entry->d_name) + 1;
        snprintf (filename, flen, "%s/%s", dirname, entry->d_name);

        int rlen = readlink(filename, linkname, llen - 1);
        if (rlen == -1)
            continue;

        assert(rlen < llen);
        linkname[llen] = 0;

        if (strncmp(linkname, "socket:[", 8) == 0) {
            char* ptr = linkname + 8;
            uint64_t inode = atol(ptr);
            procMgr_->updateMap(inode, this);
        }
    }

    closedir(dir);
    return 0;
}


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  ProcessManager
 *  Description:  
 * =====================================================================================
 */
ProcessManager::ProcessManager ( ) 
{
    this->unknownProc_ = new Process(-1, "");
    if (Process::procMgr_ == NULL)
        Process::procMgr_ = this;
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  ~ProcessManager
 *  Description:  
 * =====================================================================================
 */
ProcessManager::~ProcessManager () {
#ifdef PERF_DEBUG
    std::cout << "cleaning up for ProcessManager\n";
#endif

    this->inodeToProcMap_.clear();

    for (unsigned long i = 0; i < procList_.size(); i++) {
        delete procList_[i];
    }

    procList_.clear();

    delete unknownProc_;

}


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  refreshMap
 *  Description:  refresh inode->process map by rereading the fd directory of
 *                each monitored process
 * =====================================================================================
 */
void ProcessManager::refreshMap() {
    inodeToProcMap_.clear();
    for (std::vector<Process *>::iterator it = procList_.begin(); 
            it != procList_.end(); ) {
        if ((*it)->updateNetworkSocket() < 0)
        {
            std::cout << "update inode map for process " << (*it)->pid_ << " failed\n";
            std::cout << "please check the pid directory\n";

            delete(*it);
            it = procList_.erase(it);
        } else {
            it++;
        }
    }
}


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  findProcess
 *  Description:  find the process opening the given inode from the
 *                inode->process map; if not find, refresh the map
 * =====================================================================================
 */
Process * ProcessManager::findProcess (uint64_t inode)
{
    if (inode == 0)
        return unknownProc_;

    Process * process = this->inodeToProcMap_[inode];
    if ( process == NULL)
        this->refreshMap();
    else
        return process;

    process = this->inodeToProcMap_[inode];
    if (process == NULL)
        process = this->unknownProc_;

    return process;
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  containMap
 *  Description:  find if the given pair of <inode, process> is contained in
 *                the map
 * =====================================================================================
 */

bool ProcessManager::containMap(uint64_t inode, Process * process) {
    return (this->inodeToProcMap_[inode] == process);
}

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  stringComparator
 *  Description:  compare two strings based on there integer value
 * =====================================================================================
 */
    bool
stringComparator (char* a, char* b)
{
    return (atoi(a) < atoi(b));
}		/* -----  end of function stringComparator  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  updateProcessList
 *  Description:  update the process to be monitored
 * =====================================================================================
 */
void ProcessManager::updateProcessList(std::map<int, std::string>& pidList)
{
    int numProc = pidList.size();
    std::unique_lock<std::mutex> lock(globalMutex_);

    int oldProcListSize = this->procList_.size();
    std::vector<int> newProcList;
    std::vector<int> oldProcList;

#ifdef PERF_DEBUG
    fprintf(stdout, "no_new_proc = %d, no_old_proc = %d\n", numProc, oldProcListSize);
#endif

    std::vector<int> procListToDelete(oldProcListSize);
    std::vector<int> procListToAdd(numProc);

    for (std::map<int, std::string>::iterator it = pidList.begin();
            it != pidList.end(); it++)
    {
        newProcList.push_back(it->first);
    }

    for (int i = 0; i < oldProcListSize; i++) 
        oldProcList.push_back(procList_[i]->pid_);

#ifdef PERF_DEBUG
    std::cout << newProcList.size() << " processes found\n";
    std::cout << oldProcList.size() << " processes existed\n";
#endif

    std::sort(newProcList.begin(), newProcList.end());
    std::sort(oldProcList.begin(), oldProcList.end());

    std::vector<int>::iterator it;
    it = std::set_difference(newProcList.begin(), newProcList.end(), oldProcList.begin(), oldProcList.end(), procListToAdd.begin());
    procListToAdd.resize(it-procListToAdd.begin());

    it = std::set_difference(oldProcList.begin(), oldProcList.end(), newProcList.begin(), newProcList.end(), procListToDelete.begin());
    procListToDelete.resize(it-procListToDelete.begin());

#ifdef PERF_DEBUG
    std::cout << procListToAdd.size() << " processes to add\n";
    std::cout << procListToDelete.size() << " processes to delete\n";
#endif

    // remove the processes that are no longer to be monitored
    for (std::vector<int>::iterator it1 = procListToDelete.begin(); it1 != procListToDelete.end(); it1++) {
        for (std::vector<Process *>::iterator it2 = procList_.begin(); it2 != procList_.end(); it2++ ) {
            if (*it1 == (*it2)->pid_)
            {
                delete (*it2);
                procList_.erase(it2);
                break;
            }
        }
    }

    // establish new processes
    for (std::vector<int>::iterator it = procListToAdd.begin(); it != procListToAdd.end(); it++) {
        Process *proc = new Process(*it, pidList[*it]);
        this->procList_.push_back(proc);
    }

    this->refreshMap(); // refresh inode-to-proc map as new processes are set up
     
    for (std::vector<Process *>::iterator it = procList_.begin(); it != procList_.end(); it++)
    {
        // see if there are any exsiting connections belonging to 
        // new processes and transfer their ownership  
        if ((*it)->connList_.empty())
            transferConnection(*it);

        (*it)->updateSystemUsage();
    }

}


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  transferConnection
 *  Description:  
 * =====================================================================================
 */
void
ProcessManager::transferConnection (Process * proc) 
{
    uint64_t inode;
    for (std::vector<Connection *>::iterator it = unknownProc_->connList_.begin();
            it != unknownProc_->connList_.end(); )
    {
        inode = ConnectionManager::getConnMgr()->findInode(*it);

        if (this->inodeToProcMap_[inode] == proc) {
            // transfer the ownership of this connection
            proc->addConnection(*it);
            //(*it)->setOwner(proc);
            it = unknownProc_->connList_.erase(it);
        } else {
            it++;
        }

    }

}



/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  countSlotLen
 *  Description:  
 * =====================================================================================
 */
    void
ProcessManager::countSlotLen (  )
{
    struct timeval ctime;

    gettimeofday(&ctime, NULL);

    lastSlotLen_ = (ctime.tv_sec + ((double)(ctime.tv_usec))/1000000);  
    lastSlotLen_-= (lastSlotTime_.tv_sec + ((double)lastSlotTime_.tv_usec)/1000000);

#ifdef PERF_DEBUG
    std::cout << "the lenght of last time lost is " << lastSlotLen_ << std::endl;
#endif

    lastSlotTime_ = ctime;

    return ;
}		/* -----  end of function countSlotLen  ----- */



/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  countCpuUsage
 *  Description:  
 * =====================================================================================
 */
    void
ProcessManager::countCpuUsage (  )
{
    FILE *file;
    char statFile[20];
    char stat[4096];
    char delimiter = ' ';
    size_t pos;

    // then we calculate the total cpu time during last time slot
    // which can be read from /proc/stat 
    sprintf(statFile, "/proc/stat");
    file = fopen(statFile, "r");
    if (file == NULL) {
        std::cerr << "unable to open file: " << statFile << "\n";
        return;
    }

    // get the first line of the opened file
    if (fgets(stat, 4096, file) != stat){
      std::cout << "cannot read file stat\n";
    }

    stat[strlen(stat) - 1] = '\0';

    // this line begines with the word "cpu", followed by 10 numbers,
    // each of which is the time spent in a specific mode
    // skip the first 4 characteres: "cup ", so that str onl contains
    // the 10 number items
    std::string str(stat); 

    uint64_t time = 0;
    str = str.substr(str.find(delimiter) + 1); // skip the first field

    while (!str.empty()) {
        time += std::stoull(str, &pos);
        str = str.substr(pos);
    }

    this->cpuTime_ = time - this->lastCpuTime_;
    this->lastCpuTime_ = time;

    fclose(file);
}		/* -----  end of function countCpuUsage  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  refresh
 *  Description:  refresh the system usage for processes
 * =====================================================================================
 */
/*
  void
ProcessManager::refresh (int numProc, char ** pidList)
{
    // first update the lenght of last slot time and system cpu usage
    countSlotLen();
    countCpuUsage();

    // then update the list of monitored process and the system usage of these
    // processes
    updateProcessList(numProc, pidList);

    // finally output the updated system usage
    printProcessInfo();

    return ;
}*/	
/* -----  end of function refresh  ----- */
   
int ProcessManager::refresh (std::map<int, std::string>& pidList, char* buf)
{
    // first update the lenght of last slot time and system cpu usage
    countSlotLen();
    countCpuUsage();

    // then update the list of monitored process and the system usage of these
    // processes
    updateProcessList(pidList);

    // finally output the updated system usage
    printProcessInfo();

    return printProcessInfoToBuf(buf);
}		/* -----  end of function refresh  ----- */

/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  printProcessInfo
 *  Description:  output the system usage of processes 
 * =====================================================================================
 */
    void
ProcessManager::printProcessInfo (  )
{
    uint64_t curTime = curTime_.load();

#ifdef PERF_DEBUG
        printf("cur_slot = %lu cpu_time = %llu slot_len = %f\n", curTime_.load(), cpuTime_, lastSlotLen_);
#endif

    printf("  pid        component  cpu%%   vmem    rss   read  write   sent   recv\n");
    int mib = 1<<20;
    double unit = mib * this->lastSlotLen_;
    for (std::vector<Process *>::iterator it = procList_.begin(); it != procList_.end(); 
            it++) 
    {
        if ((*it)->startTime_ == curTime)
        {
            continue; // skip the display for the process that were just found
        }

        printf("%5d ", (*it)->pid_);

        printf("%10s ", (*it)->name_.c_str());

        // cpu usage
        printf("%4.1f%% ", 100 * ((double)((*it)->utime_ + (*it)->stime_))/this->cpuTime_);

        // memory usage
        printf("%6lu %6lu ", ((*it)->vmSize_)/mib, ((*it)->rssSize_)/mib);

        // io usage
        printf("%6d %6d ", (int)((*it)->bytesReadFromDisk_/unit),  (int)((*it)->bytesWrittenToDisk_/unit));

        // network throughput
        printf("%6d %6d\n", (int)((*it)->bytesSent_/unit), (int)((*it)->bytesRecv_/unit));
    }

    printf ("\n");
}		/* -----  end of function printProcessInfo  ----- */


/* 
 * ===  FUNCTION  ======================================================================
 *         Name:  printProcessInfoToBuf
 *  Description:  output the system usage of processes to in-mem buffer 
 * =====================================================================================
 */
int ProcessManager::printProcessInfoToBuf (char* buf){
    
    uint64_t curTime = curTime_.load();
    struct ProcInfo * p;
    int len = 0;

    p = (struct ProcInfo *)buf;

    int mib = 1<<20;
    double unit = mib * this->lastSlotLen_;
    for (std::vector<Process *>::iterator it = procList_.begin(); it != procList_.end(); it++) {
        if ((*it)->startTime_ == curTime)
            continue; // skip the display for the process that were just found
        
        // proc name
        sprintf(p->name, "%.5d:%.17s", (*it)->pid_, (*it)->name_.c_str());
        // cpu usage
        p->cpu = 100 * ((double)((*it)->utime_ + (*it)->stime_))/this->cpuTime_;
        // memory usage
        p->mem_v = ((*it)->vmSize_)/mib;
        p->mem_r = ((*it)->rssSize_)/mib;
        // io usage
        p->io_read = (int)(((*it)->bytesReadFromDisk_)/(unit));
        p->io_write = (int)(((*it)->bytesWrittenToDisk_)/(unit));
        // network throughput
        p->net_send = (int)(((*it)->bytesSent_)/(unit));
        p->net_recv = (int)(((*it)->bytesRecv_)/(unit));

        p++;
        len += sizeof(struct ProcInfo);
    }

    return len;
}		/* -----  end of function printProcessInfoToBuf  ----- */
