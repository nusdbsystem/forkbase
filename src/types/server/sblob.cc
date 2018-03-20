// Copyright (c) 2017 The Ustore Authors.

#include "types/server/sblob.h"

#include "node/blob_node.h"
#include "node/node_builder.h"

namespace ustore {

SBlob::SBlob(std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer,
    const Hash& root_hash) noexcept : UBlob(loader), chunk_writer_(writer) {
  SetNodeForHash(root_hash);
}

SBlob::SBlob(std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer,
    const Slice& data) noexcept : UBlob(loader), chunk_writer_(writer) {
  if (data.empty()) {
    ChunkInfo chunk_info = BlobChunker::Instance()->Make({});
    chunk_writer_->Write(chunk_info.chunk.hash(), chunk_info.chunk);
    SetNodeForHash(chunk_info.chunk.hash());
  } else {
    NodeBuilder nb(chunk_writer_, BlobChunker::Instance(),
                   MetaChunker::Instance());
    FixedSegment seg(data.data(), data.len(), 1);
    nb.SpliceElements(0, &seg);
    Hash root_hash(nb.Commit());
    SetNodeForHash(root_hash);
  }
}

Hash SBlob::Splice(size_t pos, size_t num_delete, const byte_t* data,
                   size_t num_append) const {
  NodeBuilder nb(root_node_->hash(), pos, chunk_loader_.get(), chunk_writer_,
                 BlobChunker::Instance(), MetaChunker::Instance());
  FixedSegment seg(data, num_append, 1);
  nb.SpliceElements(num_delete, &seg);
  return nb.Commit();
}

}  // namespace ustore
