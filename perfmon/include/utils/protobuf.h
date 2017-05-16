#ifndef INCLUDE_PROTO_BUFFER_H
#define INCLUDE_PROTO_BUFFER_H

#include <time.h>
#include <queue>
#include "proto.h"

using std::queue;

struct ProtoBufferNode{
  //node name
  char node[24];
  //captured time
  time_t t_monitor;
  //head of ProcInfo list
  ProcInfo *head;
  //capacity of current ProcInfo list
  int capacity;
  //valid number of ProcInfo nodes
  int size;
  //next pointer
  ProtoBufferNode* next;
};

class ProtoBuffer{
  private:
    int BUFFERED_TIME;
    unsigned int RESERVED_NODE;
    int UNIT;
    queue<ProtoBufferNode*> freed_list, used_list;

  public:
    ProtoBuffer();
    ~ProtoBuffer();
    void put_back(const char *node, const char *buf, int len);
    ProtoBufferNode* get_front();
  private:
    ProtoBufferNode* allocate(int size);
};
#endif
