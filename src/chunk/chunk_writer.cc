// Copyright (c) 2017 The Ustore Authors.

#include "chunk/chunk_writer.h"
#include "cluster/chunk_client.h"
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
    // check if already exist
    bool exists;
    auto stat = client_->Exists(key, &exists);
    CHECK(stat == ErrorCode::kOK) << "Failed to check remote chunk";
    if (exists) return true;
    // send chunk
    stat = client_->Put(key, chunk);
    CHECK(stat == ErrorCode::kOK) << "Failed to put remote chunk";
    return true;
  }
  LOG(FATAL) << "Failed to write chunk";
  return false;
}

}  // namespace ustore
