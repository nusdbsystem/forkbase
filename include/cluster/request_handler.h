// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_REQUEST_HANDLER_H_
#define USTORE_CLUSTER_REQUEST_HANDLER_H_

#include <condition_variable>
#include <mutex>
#include <vector>
#include "net/net.h"
#include "proto/messages.pb.h"
#include "spec/slice.h"
#include "hash/hash.h"

namespace ustore {

using google::protobuf::Message;

class WorkerList;

/**
 * A unit on the response queue. Each client request thread (RequestHandler)
 * waits on one of this object. The thread goes to sleep waiting for has_msg
 * condition to hold true. The has_msg variable will be set by the registered
 * callback method of the network thread
 *
 * This object is used under the assumption that the client issues requests in
 * a synchronous manner. As a result, the msg must be cleared before another
 * response is set.
 */
struct ResponseBlob {
  std::mutex lock;
  std::condition_variable condition;
  bool has_msg;
  Message *message = nullptr;
};

/**
 * Main entrance to the storage. It interfaces with the client (same process),
 * the master and the worker. It has 3 main tasks:
 *
 * 1. Maintain a list of worker, which is synced with the master.  
 * 2. Contain storage APIs to be invoked by the client. For each method it 
 * forwards the corresponding request to the appropriate worker, then waits
 * for a response.
 * 3. When the response indicates error (INVALID_RANGE, for example), it
 * syncs with the master.
 *
 * Each RequestHandler thread processes request synchronously, but responses
 * arrive asynchronously from the network. To route the response back to the
 * correct thread, we use thread ID to identify the message. That is:
 *    + Each UStoreMessage request now contains a field named "source"
 *    + After sending the message, the thread waits on a ResponseBlob
 *      (associated with this "source")
 *    + When a UStoreMessage response arrives, the network thread (callback)
 *      checks for the source and wakes up the corresponding ResponseBlob
 *
 * Basically the old ClientService, a client can invoke RequestHandler in
 * multiple threads, like this:
 *
 *    void ClientThread(node_id_t master, int id) {
 *      RequestHandler *rh = new RequestHandler(...);
 *      while (true) {
 *        status = rh->Get(..); // synchronous
 *        if (status!=SUCESS)
 *          rh->Get(..); // try again
 *        ...         
 *      }
 *    }
 */

class RequestHandler {
 public:
  RequestHandler(const node_id_t& master, int id, Net *net, ResponseBlob *blob,
      WorkerList* workers)
    : master_(master), id_(id), net_(net), res_blob_(blob), workers_(workers) {}
  ~RequestHandler();

  /**
   * Storage APIs. The returned Message contains the Status field indicate
   * if it is successful or not. The calling function MUST delete the response
   * message after use.
   */
  Message* Put(const Slice &key, const Slice &value,
               const Hash &version, bool forward = false, bool force = false);
  Message* Put(const Slice &key, const Slice &value, const Slice &branch,
               bool forward = false, bool force = false);

  Message* Get(const Slice &key, const Slice &branch);
  Message* Get(const Slice &key, const Hash &version);

  Message* Branch(const Slice &key, const Slice &old_branch,
                  const Slice &new_branch);
  Message* Branch(const Slice &key, const Hash &version,
                  const Slice &new_branch);

  Message* Move(const Slice &key, const Slice &old_branch,
                const Slice &new_branch);

  Message* Merge(const Slice &key, const Slice &value,
                 const Slice &target_branch, const Slice &ref_branch,
                 bool forward = false, bool force = false);
  Message* Merge(const Slice &key, const Slice &value,
                 const Slice &target_branch, const Hash &ref_version,
                 bool forward = false, bool force = false);

  inline int id() const noexcept { return id_; }

 private:
  // send request to a node. Return false if there are
  // errors with network communication.
  bool Send(const Message *msg, const node_id_t& node_id);
  // wait for response, and take ownership of the message.
  Message* WaitForResponse();
  // sync the worker list, whenever the storage APIs return error
  bool SyncWithMaster();

  // helper methods for creating messages
  UStoreMessage *CreatePutRequest(const Slice &key, const Slice &value,
                                  bool forward, bool force);
  UStoreMessage *CreateGetRequest(const Slice &key);
  UStoreMessage *CreateBranchRequest(const Slice &key,
                                     const Slice &new_branch);
  UStoreMessage *CreateMergeRequest(const Slice &key, const Slice &value,
                      const Slice &target_branch, bool forward, bool force);

  int id_ = 0;  // thread identity, in order to identify the waiting thread
  node_id_t master_;  // address of the master node
  Net *net_ = nullptr;  // for network communication
  WorkerList *workers_ = nullptr;  // lists of workers to which requests are
                                   // dispatched
  ResponseBlob *res_blob_ = nullptr;  // response blob
};

/**
 * List of workers and their key ranges. It is initialized and updated
 * with the information from the Master. The RequestHandler uses this to
 * determine where to send the requests to. One WorkerList object is shared
 * by multiple RequestHandler.
 *
 * Different partition strategies may implement the list differently, especially
 * regarding the mapping from key to worker ID (Get_Worker method).
 */
class WorkerList {
 public:
    explicit WorkerList(const std::vector<RangeInfo> &workers);
    ~WorkerList() {}

    /**
     * Invoked whenever the list is out of date.
     */
    bool Update(const std::vector<RangeInfo> &workers);
    /**
     * Return the ID (address string) of the worker node whose key range
     * contains the given key.
     * It always returns a valid ID, but the node may have gone offline. The calling
     * function is responsible for updating the list.
     */
    node_id_t GetWorker(const Slice& key);

 private:
    // should be sorted by the range
    std::vector<RangeInfo> workers_;
};

/**
 * Workload generator, that feeds requests to NextRequest(.) method
 * Currently, there are 2 types: a default Workload that *should* generate
 * requests from a distribution (like a driver in YCSB), and a TestWorkload
 * for testing.
 * Multiple client threads may share the same workload object.
 * When ClientService is to served as backend to a HTTP server, for example,
 * Workload can be extended to  
 */
class Workload {
 public:
  // return true if the request is processed successfully
  virtual bool NextRequest(RequestHandler *reqhl) = 0;
};

class RandomWorkload {
 public:
  RandomWorkload() {}
  bool NextRequest(RequestHandler *reqhl);
};

class TestWorkload : public Workload {
 public:
  TestWorkload(int nthreads, int nreqs);
  bool NextRequest(RequestHandler *reqhl);
 private:
  std::vector<int> req_idx_;
  int nthreads_, nrequests_;
};
}  // namespace ustore

#endif  // USTORE_CLUSTER_REQUEST_HANDLER_H_
