// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_CHUNK_SERVICE_H_
#define USTORE_CLUSTER_CHUNK_SERVICE_H_

#include "cluster/service.h"
#include "proto/messages.pb.h"
#include "store/chunk_store.h"
#include "utils/env.h"

namespace ustore {

/**
 * The server side of chunk service, serving requests for ChunkDb requests.
 */
class ChunkService : public Service {
 public:
  explicit ChunkService(const node_id_t& addr)
    : Service(addr, true), store_(store::GetChunkStore()) {}
  ~ChunkService() = default;

  void HandleRequest(const void *msg, int size, const node_id_t& source)
    override;

 protected:
  CallBack* RegisterCallBack() override;

 private:
  void HandleGetChunkRequest(const UMessage& umsg, ResponsePayload* reponse);
  void HandlePutChunkRequest(const UMessage& umsg, ResponsePayload* response);
  void HandleExistChunkRequest(const UMessage& umsg,
                               ResponsePayload* response);

  ChunkStore* const store_;
};
}  // namespace ustore

#endif  // USTORE_CLUSTER_CHUNK_SERVICE_H_
