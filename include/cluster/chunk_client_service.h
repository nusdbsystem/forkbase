// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_CHUNK_CLIENT_SERVICE_H_
#define USTORE_CLUSTER_CHUNK_CLIENT_SERVICE_H_

#include "cluster/client_service.h"
#include "cluster/chunkdb.h"
#include "utils/env.h"

namespace ustore {

class ChunkClientService : public ClientService {
 public:
  ChunkClientService()
    : ClientService(&ptt_),
      ptt_(Env::Instance()->config().chunk_server_file(), "") {}
  ~ChunkClientService() = default;

  ChunkDb CreateChunkDb();

 protected:
  CallBack* RegisterCallBack() override;

 private:
  const Partitioner ptt_;
};

}  // namespace ustore

#endif  // USTORE_CLUSTER_CHUNK_CLIENT_SERVICE_H_
