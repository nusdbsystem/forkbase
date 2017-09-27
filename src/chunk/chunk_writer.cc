// Copyright (c) 2017 The Ustore Authors.

#include "chunk/chunk_writer.h"
#include "cluster/partitioner.h"

namespace ustore {

bool LocalChunkWriter::Write(const Hash& key, const Chunk& chunk) {
  return cs_->Put(key, chunk);
}

bool PartitionedChunkWriter::Write(const Hash& key, const Chunk& chunk) {
  int id = ptt_->GetDestId(key);
  if (id == ptt_->id()) {
    return cs_->Put(key, chunk);
  } else {
    // TODO(anh): need to write that chunk to remote node
  }
  LOG(FATAL) << "Failed to write chunk";
  return false;
}

}  // namespace ustore
