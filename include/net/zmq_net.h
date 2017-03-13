// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NET_ZMQ_NET_H_
#define USTORE_NET_ZMQ_NET_H_

#include <mutex>
#include "net/net.h"

namespace ustore {

// Network wrapper based on zeromq
class ZmqNet : public Net {
 public:
  explicit ZmqNet(const node_id_t& id);
  NetContext* CreateNetContext(const node_id_t& id) override;

  /**
   * Start several network threads:
   * + the current one runs infinite loop to receive message from router socket
   * + another back-end thread for processing the received message
   * + another thread for send message out to other nodes
   */
  void Start() override;
  void Stop() override;
  inline bool IsRunning() const { return is_running_; }

 private:
  void *recv_sock_, *backend_sock_;  // router and backend socket
  void *socket_ctx_;
  bool is_running_;
};

class ZmqNetContext : public NetContext {
 public:
  ZmqNetContext(const node_id_t& src, const node_id_t& dest);
  ~ZmqNetContext();

  // implementation of the methods inherited from NetContext
  ssize_t Send(void* ptr, size_t len, CallBackProc* func = nullptr,
               void* app_data = nullptr);
  void RegisterRecv(CallBackProc* func, void* app_data);

 private:
  // process the received msg
  void Dispatch(void *msg, int size);

  std::mutex recv_lock_, send_lock_;
  void *send_sock_, *send_ctx_;
};

}  // namespace ustore

#endif  // USTORE_NET_ZMQ_NET_H_
