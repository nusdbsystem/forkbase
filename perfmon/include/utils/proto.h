#ifndef INCLUDE_PROTO_H
#define INCLUDE_PROTO_H

struct ProcInfo{
  //name of the process
  char name[24];
  //cpu utilization in percentage
  double cpu;
  //memory of virtual and rss
  unsigned long mem_v;
  unsigned long mem_r;
  //disk usage of read and write
  int io_read;
  int io_write;
  //network usage of send and receive
  int net_send;
  int net_recv;
};

#endif
