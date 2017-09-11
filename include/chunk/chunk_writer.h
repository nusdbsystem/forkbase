// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CHUNK_CHUNK_WRITER_H_
#define USTORE_CHUNK_CHUNK_WRITER_H_

#include "chunk/chunk.h"
#include "hash/hash.h"
#include "store/chunk_store.h"
#include "utils/noncopyable.h"

namespace ustore {

/** ChunkWriter is responsible to persist chunks into storage. */
class ChunkWriter : private Noncopyable {
 public:
  virtual ~ChunkWriter() = default;

  virtual bool Write(const Hash& key, const Chunk& chunk) = 0;

 protected:
  ChunkWriter() = default;
};

class ServerChunkWriter : public ChunkWriter {
 public:
  // let ServerChunkWriter call chunkStore internally
  ServerChunkWriter() : cs_(store::GetChunkStore()) {}
  ~ServerChunkWriter() = default;

  bool Write(const Hash& key, const Chunk& chunk) override;

 private:
  ChunkStore* const cs_;
};

}  // namespace ustore

#endif  // USTORE_CHUNK_CHUNK_WRITER_H_
