// Copyright (c) 2017 The Ustore Authors.

#include "types/server/sblob.h"

#include "node/blob_node.h"
#include "node/node_builder.h"

namespace ustore {

SBlob::SBlob(const Hash& root_hash, ChunkWriter* writer) noexcept :
    UBlob(std::make_shared<ServerChunkLoader>()), chunk_writer_(writer) {
  SetNodeForHash(root_hash);
}

SBlob::SBlob(const Slice& data, ChunkWriter* writer) noexcept :
    UBlob(std::make_shared<ServerChunkLoader>()), chunk_writer_(writer) {
  if (data.empty()) {
    ChunkInfo chunk_info = BlobChunker::Instance()->Make({});
    chunk_writer_->Write(chunk_info.chunk.hash(), chunk_info.chunk);
    SetNodeForHash(chunk_info.chunk.hash());
  } else {
    NodeBuilder nb(chunk_writer_, BlobChunker::Instance(), true);
    FixedSegment seg(data.data(), data.len(), 1);
    nb.SpliceElements(0, &seg);
    Hash root_hash(nb.Commit());
    SetNodeForHash(root_hash);
  }
}

Hash SBlob::Splice(size_t pos, size_t num_delete, const byte_t* data,
                   size_t num_append) const {
  NodeBuilder nb(root_node_->hash(), pos, chunk_loader_.get(),
                 chunk_writer_, BlobChunker::Instance(), true);
  FixedSegment seg(data, num_append, 1);
  nb.SpliceElements(num_delete, &seg);
  return nb.Commit();
}

}  // namespace ustore
