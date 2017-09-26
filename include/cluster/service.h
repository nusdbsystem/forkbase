// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_SERVICE_H_
#define USTORE_CLUSTER_SERVICE_H_

#include <memory>
#include <string>
#include "net/net.h"
#include "utils/noncopyable.h"

namespace ustore {

/**
 * Service is an abstracted class to handle request from server side.
 * A Service receives requests from Client and invokes corresponding
 * classes to process the message.
 * Derived class from Service should provide impl for handling requests.
 */
class Service : private Noncopyable {
 public:
  explicit Service(const node_id_t& addr) : node_addr_(addr) {}
  virtual ~Service() = default;

  void Start();
  void Stop();

  /**
   * Handle requests:
   * 1. It parse msg into a UStoreMessage
   * 2. Invoke the processing logic correspondingly.
   * 3. Construct a response and send back to source.
   */
  virtual void HandleRequest(const void *msg, int size,
                             const node_id_t& source) = 0;

 protected:
  inline void Send(const node_id_t& source, byte_t* ptr, int len) {
    net_->GetNetContext(source)->Send(ptr, static_cast<size_t>(len));
  }

  // allocate a net::CallBack instance
  virtual CallBack* RegisterCallBack() = 0;
  const node_id_t node_addr_;

 private:
  std::unique_ptr<CallBack> cb_;
  std::unique_ptr<Net> net_;
};
}  // namespace ustore

#endif  // USTORE_CLUSTER_SERVICE_H_
