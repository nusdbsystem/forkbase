// Copyright (c) 2017 The Ustore Authors.

#ifndef INCLUDE_NET_ZMQ_NET_H_
#define INCLUDE_NET_ZMQ_NET_H_

#include <mutex>
#include "net/net.h"

namespace ustore {

// Network wrapper based on zeromq
class ZmqNet : public Net {
 public:
  explicit ZmqNet(const node_id_t& id, int nthreads = 1);
  NetContext* CreateNetContext(const node_id_t& id) override;

  /**
   * Start several network threads:
   * + the current one runs infinite loop to receive message from router socket
   * + another back-end thread for processing the received message
   * + another thread for send message out to other nodes
   */
  void Start() override;
  void Stop() override;
  inline bool IsRunning() const noexcept { return is_running_; }

  void RegisterRecv(CallBackProc* func, void* handler) override;

  // process the received msg
  void Dispatch(const node_id_t& source, const void *msg, int size);

 private:
  void *recv_sock_, *backend_sock_;  // router and backend socket
  void *socket_ctx_;
  bool is_running_;
  int nthreads_;  // number of processing threads
};

class ZmqNetContext : public NetContext {
 public:
  ZmqNetContext(const node_id_t& src, const node_id_t& dest);
  ~ZmqNetContext();

  // implementation of the methods inherited from NetContext
  ssize_t Send(const void* ptr, size_t len, CallBackProc* func = nullptr,
               void* handler = nullptr) override;

 private:
  //std::mutex recv_lock_, send_lock_;
  std::mutex send_lock_;
  void *send_sock_, *send_ctx_;
};

}  // namespace ustore

#endif  // INCLUDE_NET_ZMQ_NET_H_
