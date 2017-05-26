// Copyright (c) 2017 The Ustore Authors.

#include "types/ublob.h"

#include <cstring>
#include "chunk/chunker.h"
#include "node/blob_node.h"
#include "node/cursor.h"
#include "utils/logging.h"

namespace ustore {

bool UBlob::SetNodeForHash(const Hash& root_hash) {
  const Chunk* chunk = chunk_loader_->Load(root_hash);
  if (chunk == nullptr) return false;

  if (chunk->type() == ChunkType::kMeta) {
    root_node_.reset(new MetaNode(chunk));
    return true;
  } else if (chunk->type() == ChunkType::kBlob) {
    root_node_.reset(new BlobNode(chunk));
    return true;
  } else {
    LOG(FATAL) << "Cannot be other chunk type for Ublob.";
  }
  return false;
}

Hash UBlob::Insert(size_t pos, const byte_t* data,
                         size_t num_insert) const {
  return Splice(pos, 0, data, num_insert);
}

Hash UBlob::Delete(size_t pos, size_t num_delete) const {
  return Splice(pos, num_delete, nullptr, 0);
}

Hash UBlob::Append(const byte_t* data, size_t num_append) const {
  return Splice(size(), 0, data, num_append);
}

size_t UBlob::Read(size_t pos, size_t len, byte_t* buffer) const {
  if (pos >= size()) {
    LOG(WARNING) << "Read Pos exceeds Blob Size. ";
    return 0;
  }
  NodeCursor cursor(root_node_->hash(), pos, chunk_loader_.get());
  size_t total_copy_byte = 0;
  do {
    size_t pre_copy_byte = total_copy_byte;
    size_t chunk_copy_byte = 0;
    const byte_t* chunk_copy_start = cursor.current();
    do {
      chunk_copy_byte += cursor.numCurrentBytes();
      total_copy_byte += cursor.numCurrentBytes();
      if (total_copy_byte == len) break;
    } while (cursor.Advance(false));
    std::memcpy(buffer + pre_copy_byte, chunk_copy_start, chunk_copy_byte);
    if (total_copy_byte == len) break;
  } while (cursor.Advance(true));

  return total_copy_byte;
}

}  // namespace ustore
