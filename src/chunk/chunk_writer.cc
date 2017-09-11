// Copyright (c) 2017 The Ustore Authors.

#include "chunk/chunk_writer.h"

namespace ustore {

bool ServerChunkWriter::Write(const Hash& key, const Chunk& chunk) {
  return cs_->Put(key, chunk);
}

}  // namespace ustore
