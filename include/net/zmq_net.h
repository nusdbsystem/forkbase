// Copyright (c) 2017 The Ustore Authors.
/*
 * zmq_net.h
 *
 *  Created on: Mar 10, 2017
 *      Author: zhanghao
 */

#ifndef INCLUDE_NET_ZMQ_NET_H_
#define INCLUDE_NET_ZMQ_NET_H_

#include <mutex>
#include "net.h"

namespace ustore {

// Network wrapper based on zeromq
class ZmqNet : public Net {
 public:
  ZmqNet(const node_id_t id);
  NetContext* CreateNetContext(node_id_t id);

  /**
   * Start several network threads:
   * + the current one runs infinite loop to receive message from router socket
   * + another back-end thread for processing the received message
   * + another thread for send message out to other nodes
   */
  void Start();
  void Stop();

  bool IsRunning() { return is_running_; }
 private:
  void *recv_sock_, *backend_sock_;  // router and backend socket
  void *socket_ctx_;
  bool is_running_;
};

class ZmqNetContext : public NetContext {
 public:
  ZmqNetContext(node_id_t src, node_id_t dest);
  ~ZmqNetContext();

  //implementation of the methods inherited from NetContext
  ssize_t Send(void* ptr, size_t len, CallBackProc* func = nullptr,
               void* app_data = nullptr);
  void RegisterRecv(CallBackProc* func, void* app_data);

 private:
  std::mutex recv_lock_, send_lock_;
  void *send_sock_, *send_ctx_;

  //process the received msg
  void Dispatch(void *msg, int size);
};

}  // namespace ustore

#endif /* INCLUDE_NET_ZMQ_NET_H_ */
