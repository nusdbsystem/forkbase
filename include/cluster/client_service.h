// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_CLIENT_SERVICE_H_
#define USTORE_CLUSTER_CLIENT_SERVICE_H_

#include <vector>
#include "cluster/request_handler.h"
#include "net/net.h"

namespace ustore {
/**
 * The client sends requests to this ClientService, which then forwards 
 * them to the storage.  
 * 
 * The client threads (ClientThread) pulls request from workload->NextRequest(.)
 * method which is modelled as a shared request queue.  
 *
 * The current design assumes the client is the same as the ClientService,
 * i.e. the ClientService issues the request themselves. Thus, Workload
 * simply generates a new request whenever it is called.
 *
 */
class ClientService {
 public:
  // Dispatching responses from the network.
  // Basically call this->HandleResponse() that wakes up client threads.
  static void ResponseDispatch(const void *msg, int size, void *handler,
                               const node_id_t& source);

  ClientService(const node_id_t& addr, const node_id_t& master,
                Workload *workload) :
      node_addr_(addr), master_(master), is_running_(false),
      workload_(workload) {}
  ~ClientService();

  // initialize the network, register callback
  virtual void Init();
  // Spawn client threads
  virtual void Start();
  // Stop: wait for client threads to join
  virtual void Stop();
  /**
   * Handle a response from workers:
   * 1. It parse msg into a UStoreMessage
   * 2. Wake up the appropriate ResponseBlob, based on the source 
   *    field, which is in [0..nthreads). No lock is needed here, because
   *    clients are synchronous (there cannot be two response to the same 
   *    client.
   * 3. Hand-off the message to whatever thread is waiting for it.
   */
  virtual void HandleResponse(const void *msg, int size,
                              const node_id_t& source);
  /**
   * Inside this thread a RequestHandle is initialized, using the following
   * information:
   *  + master
   *  + Network object (shared with other threads)
   *  + thread_id
   *  + ResponseBlob, which is responses_[id]
   */
  void ClientThread(const node_id_t& master, int thread_id);

  static int range_cmp(const RangeInfo& a, const RangeInfo& b);

 private:
  int nthreads_;  // how many RequestHandler thread it uses
  volatile bool is_running_;  // volatile to avoid caching old value
  node_id_t master_;  // master node
  node_id_t node_addr_;  // the node's address
  Net *net_ = nullptr;
  std::vector<ResponseBlob*> responses_;  // the response queue
  WorkerList *workers_ = nullptr;  // worker list
  Workload *workload_ = nullptr;
  std::vector<node_id_t> addresses_;  // worker addresses
  CallBack* cb_ = nullptr;
};

}  // namespace ustore

#endif  // USTORE_CLUSTER_CLIENT_SERVICE_H_
