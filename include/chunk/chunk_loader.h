// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CHUNK_CHUNK_LOADER_H_
#define USTORE_CHUNK_CHUNK_LOADER_H_

#include <map>
#include <string>
#include "chunk/chunk.h"
#include "hash/hash.h"
#include "spec/db.h"
#include "store/chunk_store.h"
#include "utils/noncopyable.h"

namespace ustore {

/** ChunkLoader is responsible to load chunks and cache them. */
class ChunkLoader : private Noncopyable {
 public:
  virtual ~ChunkLoader() = default;

  const Chunk* Load(const Hash& key);

 protected:
  ChunkLoader() = default;
  virtual Chunk GetChunk(const Hash& key) = 0;
  std::map<Hash, Chunk> cache_;
};

class ServerChunkLoader : public ChunkLoader {
 public:
  // let ServerChunkLoader call chunkStore internally
  ServerChunkLoader() : cs_(store::GetChunkStore()) {}
  // Delete all chunks
  ~ServerChunkLoader() = default;

 protected:
  Chunk GetChunk(const Hash& key) override;

 private:
  ChunkStore* cs_;
};

class ClientChunkLoader : public ChunkLoader {
 public:
  // need a db instance
  ClientChunkLoader(DB2* db, const Slice& key)
    : db_(db), key_(key.ToString()) {}
  // Delete all chunks
  ~ClientChunkLoader() = default;

 protected:
  Chunk GetChunk(const Hash& key) override;

 private:
  std::string key_;
  DB2* db_;
};

}  // namespace ustore

#endif  // USTORE_CHUNK_CHUNK_LOADER_H_
