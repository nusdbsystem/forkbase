// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>

#include "utils/protobuf.h"

ProtoBuffer::ProtoBuffer() {
  // buffer for one minute
  // BUFFERED_TIME = 60;
  BUFFERED_TIME = 60;
  // reserve nodes in freed_list
  RESERVED_NODE = 50;
  // size of a ProcInfo instance
  UNIT = sizeof(struct ProcInfo);
}

ProtoBuffer::~ProtoBuffer() {
  ProtoBufferNode* p;
  // release memory
  while (freed_list.size()) {
    p = freed_list.front();
    freed_list.pop();
    delete p->head;
    delete p;
  }
  while (used_list.size()) {
    p = used_list.front();
    used_list.pop();
    delete p->head;
    delete p;
  }
}

void ProtoBuffer::put_back(const char *node,
  const char *buf, int len) {
  int size = len/UNIT;
  // remove old nodes in used_list
  time_t t_cur = time(NULL);
  ProtoBufferNode* old;
  while (used_list.size()) {
    old = used_list.front();
    // remove old node
    // std::cout << "t_cur = " << t_cur << " t_monitor = "
    // << old->t_monitor << "\n";
    if (t_cur - old->t_monitor > BUFFERED_TIME) {
      used_list.pop();
      freed_list.push(old);
    } else {
     break;
    }
  }

  // allocate a new node
  ProtoBufferNode* p = allocate(size);

  // write info into the node
  strcpy(p->node, node);
  p->t_monitor = time(NULL);
  // std::cout << "time = " << p->t_monitor << "\n";
  // std::cout << "len = " << len << "\n";
  memcpy(p->head, buf, len);
  p->size = size;
  p->next = nullptr;

  // add into used_list
  if (used_list.size()) {
    ProtoBufferNode* pre = used_list.back();
    pre->next = p;
  }
  used_list.push(p);
}

ProtoBufferNode* ProtoBuffer::get_front() {
  return used_list.size() ? used_list.front() : nullptr;
}

ProtoBufferNode* ProtoBuffer::allocate(int size) {
  // get a freed node
  ProtoBufferNode* p = nullptr;
  if (freed_list.size() > RESERVED_NODE) {
    p = freed_list.front();
    freed_list.pop();
  }
  // allocate a new node
  if (p == nullptr) {
    p = new ProtoBufferNode;
    p->head = nullptr;
    p->capacity = 0;
  }
  // check to add
  if (p->capacity < size) {
    // delete small list
    if (p->head != nullptr) delete p->head;
    p->head = new ProcInfo[size];
    p->capacity = size;
  }
  return p;
}
