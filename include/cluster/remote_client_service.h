// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_REMOTE_CLIENT_SERVICE_H_
#define USTORE_CLUSTER_REMOTE_CLIENT_SERVICE_H_

#include <vector>
#include "cluster/clientdb.h"
#include "net/net.h"

namespace ustore {
/**
 * Service that handle remote client requests to the database.
 * Multiple clients/threads share the same service, thus avoiding
 * creating one connection for each client.
 * 
 * To interact with the database, the user first creates a ClientDb
 * object via CreateClientDb().
 *
 * The database servers (Worker) knows about every RemoteClientService
 * in the system, and can send responses asynchronously. 
 */
class RemoteClientService {
 public:
  // Dispatching responses from the network.
  // Basically call this->HandleResponse() that wakes up client threads.
  static void ResponseDispatch(const void *msg, int size, void *handler,
                               const node_id_t& source);

  RemoteClientService(const node_id_t& master)
      : node_addr_(""), master_(master), is_running_(false), nclients_(0) {}
  ~RemoteClientService();

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
   * Create a new ClientDb connecting to the database. 
   * Interaction with the database is through this object.
   */
  virtual ClientDb* CreateClientDb();

  static int range_cmp(const RangeInfo& a, const RangeInfo& b);

 private:
  int nclients_;  // how many RequestHandler thread it uses
  volatile bool is_running_;  // volatile to avoid caching old value
  node_id_t master_;  // master node
  node_id_t node_addr_;  // the node's address
  Net *net_ = nullptr;
  std::vector<ResponseBlob*> responses_;  // the response queue
  WorkerList *workers_ = nullptr;  // worker list
  std::vector<node_id_t> addresses_;  // worker addresses
  CallBack* cb_ = nullptr;
};

}  // namespace ustore

#endif  // USTORE_CLUSTER_REMOTE_CLIENT_SERVICE_H_
