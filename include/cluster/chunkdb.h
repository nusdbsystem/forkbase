// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CLUSTER_CHUNKDB_H_
#define USTORE_CLUSTER_CHUNKDB_H_

#include "cluster/clientdb.h"

namespace ustore {


/**
 * The client part of chunk service, providing APIs for fetching, writing and checking
 * certain chunk hash. 
 * An extension of ClientDB
 */

class ChunkDb : public ClientDb {
 public:
  ChunkDb(const node_id_t& master, int id, Net* net, ResponseBlob* blob,
           const Partitioner* ptt) : ClientDb(master, id, net, blob, ptt) {}
  ~ChunkDb() = default;

  ErrorCode Get(const Hash& hash, Chunk* chunk);
  ErrorCode Put(const Hash& hash, const Chunk& chunk);
  ErrorCode Exists(const Hash& hash, bool* exist);
 private:
  void CreateChunkRequest(const Hash& hash, UMessage *msg);
};

}  // namespace ustore

#endif  // USTORE_CLUSTER_CHUNKDB_H_
