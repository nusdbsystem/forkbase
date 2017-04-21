// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_CLIENTDB_H_
#define USTORE_CLUSTER_CLIENTDB_H_

#include <condition_variable>
#include <mutex>
#include <vector>
#include "net/net.h"
#include "proto/messages.pb.h"
#include "spec/db.h"
#include "spec/slice.h"
#include "hash/hash.h"

namespace ustore {

using google::protobuf::Message;

class WorkerList;

/**
 * A unit on the response queue. Each client request thread 
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
 * ClientDb object is created from RemoteClientService. 
 * 
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
 * Each ClientDb can be run on a separate thread, processing requests synchronously.
 * Responses arrive asynchronously from the network. To route the response back to the
 * correct thread, we use thread ID to identify the message. That is:
 *    + Each UStoreMessage request now contains a field named "source"
 *    + After sending the message, the thread waits on a ResponseBlob
 *      (associated with this "source")
 *    + When a UStoreMessage response arrives, the network thread (callback)
 *      checks for the source and wakes up the corresponding ResponseBlob
 *
 */

class ClientDb : public DB {
 public:
  ClientDb(const node_id_t& master, int id, Net *net, ResponseBlob *blob,
      WorkerList* workers)
    : master_(master), id_(id), net_(net), res_blob_(blob), workers_(workers) {}
  ~ClientDb();

  // Storage APIs. Inheritted from DB.
  ErrorCode Get(const Slice& key, const Slice& branch,
                        Value* value) override;
  ErrorCode Get(const Slice& key, const Hash& version,
                        Value* value) override;
  ErrorCode Put(const Slice& key, const Value& value,
                        const Slice& branch, Hash* version) override;
  ErrorCode Put(const Slice& key, const Value& value,
                        const Hash& pre_version, Hash* version) override;
  ErrorCode Branch(const Slice& key, const Slice& old_branch,
                           const Slice& new_branch) override;
  ErrorCode Branch(const Slice& key, const Hash& version,
                           const Slice& new_branch) override;
  ErrorCode Rename(const Slice& key, const Slice& old_branch,
                           const Slice& new_branch) override;
  ErrorCode Merge(const Slice& key, const Value& value,
                          const Slice& tgt_branch, const Slice& ref_branch,
                          Hash* version) override;
  ErrorCode Merge(const Slice& key, const Value& value,
                          const Slice& tgt_branch, const Hash& ref_version,
                          Hash* version) override;
  ErrorCode Merge(const Slice& key, const Value& value,
                          const Hash& ref_version1, const Hash& ref_version2,
                          Hash* version) override;

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
  UStoreMessage *CreatePutRequest(const Slice &key, const Value &value);
  UStoreMessage *CreateGetRequest(const Slice &key);
  UStoreMessage *CreateBranchRequest(const Slice &key,
                                     const Slice &new_branch);
  UStoreMessage *CreateMergeRequest(const Slice &key, const Value &value,
                                    const Slice &target_branch);

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
}  // namespace ustore

#endif  // USTORE_CLUSTER_CLIENTDB_H_
