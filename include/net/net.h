// Copyright (c) 2017 The Ustore Authors.

#ifndef INCLUDE_NET_NET_H_
#define INCLUDE_NET_NET_H_

#include <string>
#include <unordered_map>
#include <vector>
#include "types/type.h"
#include "utils/noncopyable.h"

namespace ustore {

typedef std::string node_id_t;

/**
 * Callback function that is invoked on receiving of a message.
 * The network thread will free the message after calling this, so if
 * the message is to be processed asynchronously, a copy must be made.
 * handler: is the pointer to the object that processes the message
 *          it is "registered" via the Net object (RegisterRecv)
 * source:  extracted from the received socket
 */ 
typedef void CallBackProc(const void *msg, int size, void* handler,
                                            const node_id_t& source);

class NetContext;

/**
 * Wrapper to all network connections. This should be created only once for each network
 * implementation (either TCP or RDMA).
 */
class Net : private Noncopyable {
 public:
  virtual ~Net() = 0;

  // create the NetContext of idth node
  virtual NetContext* CreateNetContext(const node_id_t& id) = 0;
  // create the NetContexts of all the nodes
  void CreateNetContexts(const std::vector<node_id_t>& nodes);
  virtual void Start() = 0;  // start the listening service
  virtual void Stop() = 0;  // stop the listening service

  // get the NetContext of the idth node
  inline const node_id_t& GetNodeID() const { return cur_node_; }
  inline NetContext* GetNetContext(const node_id_t& id) const {
    return netmap_.at(id);
  }

  /**
   * Register the callback function that will be called whenever there is new
   * data is received
   * func: the callback function
   * handler: pointer to a object that handle this request
   *
   * anh: originally this is a member of NetContext. But moved here to make 
   * it cleaner. First, it needs to registered only once, instead of for as many as 
   * the number of connections. Second, there is no concurrency problem, because the
   * received socket is read by only one (the main) thread. 
   */
  virtual void RegisterRecv(CallBackProc* func, void* handler) = 0;

 protected:
  Net() {}
  explicit Net(const node_id_t& id) : cur_node_(id) {}
  node_id_t cur_node_;
  std::unordered_map<node_id_t, NetContext*> netmap_;
  void *upstream_handle_ = nullptr;  // to be passed to callback
  CallBackProc* cb_ = nullptr;
};

/**
 * Generic network context representing one end-to-end connection.
 */
class NetContext {
 public:
  NetContext() {}
  // Initialize connection to another node
  NetContext(const node_id_t& src, const node_id_t& dest)
      : src_id_(src), dest_id_(dest) {}
  virtual ~NetContext() {};

  // Non-blocking APIs
  /*
   * send the data stored in ptr[0, len-1]
   * ptr: the starting pointer to send
   * len: size of the buffer
   * func: callback function after send completion (not supported)
   * handler: the application-provided handler that will be used in the callback
   *           function (not supported)
   */
  virtual ssize_t Send(const void* ptr, size_t len, CallBackProc* func = nullptr,
                       void* handler = nullptr) = 0;


  // Blocking APIs (not supported)
  virtual ssize_t SyncSend(const void* ptr, size_t len);
  virtual ssize_t SyncRecv(const void* ptr, size_t len);

  // methods to access private variables
  inline const node_id_t& srcID() const noexcept { return src_id_; }
  inline const node_id_t& destID() const noexcept { return dest_id_; }

 protected:
  node_id_t src_id_, dest_id_;
};

}  // namespace ustore

#endif  // INCLUDE_NET_NET_H_
