// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_CHUNK_SERVICE_H_
#define USTORE_CLUSTER_CHUNK_SERVICE_H_

#include "cluster/worker_service.h"
#include "utils/env.h"

namespace ustore {

/**
 * The server side of chunk service, serving requests for ChunkDb requests.
 * Direct extension of WorkerService, only differs in the network component (use different ports).
 */
class ChunkService : public WorkerService {
 public:
    ChunkService(const node_id_t& addr, const node_id_t& master, bool persist)
      : WorkerService(addr, master, persist) {
      ptt_ = Partitioner(Env::Instance()->config().chunk_server_file(), addr);
      
    }

    ~ChunkService() = default;

    void Start() override;

    void HandleRequest(const void *msg, int size,
                               const node_id_t& source) override;

 private:
    ChunkStore *store_;
    void HandleGetChunkRequest(const UMessage& umsg, ResponsePayload* reponse);
    void HandlePutChunkRequest(const UMessage& umsg, ResponsePayload* response);
    void HandleExistChunkRequest(const UMessage& umsg, ResponsePayload* response);
    bool persist_;
};
}  // namespace ustore

#endif  // USTORE_CLUSTER_CHUNK_SERVICE_H_
