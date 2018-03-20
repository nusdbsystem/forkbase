// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CHUNK_CHUNKER_H_
#define USTORE_CHUNK_CHUNKER_H_

#include <memory>
#include <vector>

#include "chunk/chunk.h"
#include "chunk/segment.h"

#include "node/rolling_hash.h"

namespace ustore {

struct ChunkInfo {
  Chunk chunk;
  // a Segment that holding a single MetaEntry bytes
  std::unique_ptr<const Segment> meta_seg;
};

class Chunker {
  // An interface to make chunk from multiple segments.
  //   Each type, e.g, Blob, MetaNode shall have one.
 public:
  virtual ChunkInfo Make(
      const std::vector<const Segment*>& segments) const = 0;
  virtual std::unique_ptr<RollingHasher> GetRHasher() const = 0;
  virtual bool isFixedEntryLen() const = 0;
  virtual ~Chunker() {}
};
}  // namespace ustore
#endif  // USTORE_CHUNK_CHUNKER_H_
