// Copyright (c) 2017 The Ustore Authors.
/*
 * net.h
 *
 *  Created on: Mar 10, 2017
 *      Author: zhanghao
 */

#ifndef INCLUDE_NET_NET_H_
#define INCLUDE_NET_NET_H_

#include <unordered_map>
#include <vector>
#include "types/type.h"

namespace ustore {

typedef std::string node_id_t;
typedef void CallBackProc(void *msg, int size, void* app_data);

/**
 * Wrapper to all network connections. This should be created only once for each network
 * implementation (either TCP or RDMA).
 */
class Net {
 public:
  Net() {
  }
  Net(const node_id_t id)
      : cur_node_(id) {
  }
  virtual ~Net() = 0;

  //create the NetContext of idth node
  virtual NetContext* CreateNetContext(node_id_t id) = 0;
  //create the NetContexts of all the nodes
  void Init(const std::vector<node_id_t>& nodes);

  //get the NetContext of the idth node
  NetContext* GetNetContext(node_id_t id) {
    return netmap_[id];
  }

  virtual void Start() = 0;  //start the listening service
  virtual void Stop() = 0;  //stop the listening service

  node_id_t GetNodeID() {
    return cur_node_;
  }

 private:
  node_id_t cur_node_;
  std::unordered_map<node_id_t, NetContext*> netmap_;
};

/**
 * Generic network context representing one end-to-end connection.
 */
class NetContext {
 public:
  NetContext() {
  }
  //Initialize connection to another node
  NetContext(node_id_t src, node_id_t dest)
      : src_id_(src),
        dest_id_(dest) {
  }
  virtual ~NetContext() = 0;

  // Non-blocking APIs
  /*
   * send the data stored in ptr[0, len-1]
   * ptr: the starting pointer to send
   * len: size of the buffer
   * func: callback function after send completion (not supported)
   * app_data: the application-provided data that will be used in the callback function (not supported)
   */
  virtual ssize_t Send(void* ptr, size_t len, CallBackProc* func = nullptr,
                       void* app_data = nullptr) = 0;

  // Register a callback function
  /*
   * register the callback function that will be called whenever there is new data received
   * func: the callback function
   * app_data: the application-provided data that will be used in the callback function
   */
  virtual void RegisterRecv(CallBackProc* func, void* app_data) = 0;

  // Blocking APIs (not supported)
  virtual ssize_t SyncSend(void* ptr, size_t len);
  virtual ssize_t SyncRecv(void* ptr, size_t& len);

  //methods to access private variables
  node_id_t GetSrcID() {
    return src_id_;
  }
  node_id_t GetDestID() {
    return dest_id_;
  }

 private:
  node_id_t src_id_, dest_id_;
  CallBackProc* cb_ = nullptr;
  void *upstream_handle_ = nullptr;  // to be passed to callback
};

}  // namespace ustore

#endif /* INCLUDE_NET_NET_H_ */
