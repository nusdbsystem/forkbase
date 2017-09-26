// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_REMOTE_CHUNK_CLIENT_SERVICE_H_
#define USTORE_CLUSTER_REMOTE_CHUNK_CLIENT_SERVICE_H_

#include "cluster/remote_client_service.h"
#include "cluster/chunkdb.h"

namespace ustore {

/**
 * Extension of RemoteClientService, for handling Chunk requests.
 */
class RemoteChunkClientService : public RemoteClientService {
 public:
  explicit RemoteChunkClientService(const node_id_t& master)
      : RemoteClientService(master) {
    ptt_ = Partitioner(Env::Instance()->config().chunk_server_file(), "");
  }
  ~RemoteChunkClientService() = default;

  void Start() override;
  ChunkDb CreateChunkDb();
};

}  // namespace ustore

#endif  // USTORE_CLUSTER_REMOTE_CHUNK_CLIENT_SERVICE_H_
