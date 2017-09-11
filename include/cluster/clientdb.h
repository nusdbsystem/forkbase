// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_CLIENTDB_H_
#define USTORE_CLUSTER_CLIENTDB_H_

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include "cluster/partitioner.h"
#include "net/net.h"
#include "proto/messages.pb.h"
#include "spec/db.h"
#include "spec/slice.h"
#include "hash/hash.h"

namespace ustore {

using google::protobuf::Message;

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
  Message* message = nullptr;
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
 *    + Each UMessage request now contains a field named "source"
 *    + After sending the message, the thread waits on a ResponseBlob
 *      (associated with this "source")
 *    + When a UMessage response arrives, the network thread (callback)
 *      checks for the source and wakes up the corresponding ResponseBlob
 *
 */

class ClientDb : public DB {
 public:
  ClientDb(const node_id_t& master, int id, Net* net, ResponseBlob* blob,
           const Partitioner* ptt)
    : master_(master), id_(id), net_(net), res_blob_(blob), ptt_(ptt) {}

  ~ClientDb() = default;

  // Storage APIs. Inheritted from DB.
  ErrorCode Get(const Slice& key, const Slice& branch, UCell* meta) const
    override;
  ErrorCode Get(const Slice& key, const Hash& version,
                UCell* meta) const override;

  ErrorCode Put(const Slice& key, const Value& value,
                const Slice& branch, Hash* version) override;
  ErrorCode Put(const Slice& key, const Value& value,
                const Hash& pre_version, Hash* version) override;

  ErrorCode Merge(const Slice& key, const Value& value,
                  const Slice& tgt_branch, const Slice& ref_branch,
                  Hash* version) override;
  ErrorCode Merge(const Slice& key, const Value& value,
                  const Slice& tgt_branch, const Hash& ref_version,
                  Hash* version) override;
  ErrorCode Merge(const Slice& key, const Value& value,
                  const Hash& ref_version1, const Hash& ref_version2,
                  Hash* version) override;

  ErrorCode ListKeys(std::vector<std::string>* keys) const override;
  ErrorCode ListBranches(const Slice& key,
                         std::vector<std::string>* branches) const override;

  ErrorCode Exists(const Slice& key, bool* exist) const override;
  ErrorCode Exists(const Slice& key, const Slice& branch, bool* exist) const
    override;

  ErrorCode GetBranchHead(const Slice& key, const Slice& branch,
                          Hash* version) const override;
  ErrorCode IsBranchHead(const Slice& key, const Slice& branch,
                         const Hash& version, bool* isHead) const override;

  ErrorCode GetLatestVersions(const Slice& key,
                              std::vector<Hash>* versions) const override;
  ErrorCode IsLatestVersion(const Slice& key, const Hash& version,
                            bool* isLatest) const override;

  ErrorCode Branch(const Slice& key, const Slice& old_branch,
                   const Slice& new_branch) override;
  ErrorCode Branch(const Slice& key, const Hash& version,
                   const Slice& new_branch) override;
  ErrorCode Rename(const Slice& key, const Slice& old_branch,
                   const Slice& new_branch) override;
  ErrorCode Delete(const Slice& key, const Slice& branch) override;

  ErrorCode GetChunk(const Slice& key, const Hash& version,
                     Chunk* chunk) const override;

  ErrorCode GetStorageInfo(std::vector<StoreInfo>* info) const override;

  inline int id() const noexcept { return id_; }

 private:
  // send request to a node. Return false if there are
  // errors with network communication.
  bool Send(const Message& msg, const node_id_t& node_id) const;
  // wait for response, and take ownership of the message.
  std::unique_ptr<UMessage> WaitForResponse() const;
  // sync the worker list, whenever the storage APIs return error
  bool SyncWithMaster();
  // helper methods for creating messages
  void CreatePutMessage(const Slice& key, const Value& value, UMessage* msg)
      const;
  void CreateGetMessage(const Slice& key, UMessage* msg) const;
  void CreateBranchMessage(const Slice& key, const Slice& new_branch,
      UMessage* msg) const;
  void CreateMergeMessage(const Slice& key, const Value& value, UMessage* msg)
      const;
  // helper methods for getting response
  ErrorCode GetEmptyResponse() const;
  ErrorCode GetVersionResponse(Hash* version) const;
  ErrorCode GetUCellResponse(UCell* value) const;
  ErrorCode GetStringListResponse(std::vector<string>* vals) const;
  ErrorCode GetVersionListResponse(std::vector<Hash>* versions) const;
  ErrorCode GetBoolResponse(bool* exists) const;
  ErrorCode GetChunkResponse(Chunk* chunk) const;
  ErrorCode GetInfoResponse(std::vector<StoreInfo>* info) const;
  inline Hash ToHash(const std::string& request) const {
    return Hash(reinterpret_cast<const byte_t*>(request.data()));
  }

  node_id_t master_;  // address of the master node
  int id_ = 0;  // thread identity, in order to identify the waiting thread
  Net* net_ = nullptr;  // for network communication
  ResponseBlob* res_blob_ = nullptr;  // response blob
  const Partitioner* ptt_;  // partitioner to route destination worker
};

}  // namespace ustore

#endif  // USTORE_CLUSTER_CLIENTDB_H_
