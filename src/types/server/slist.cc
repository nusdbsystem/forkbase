// Copyright (c) 2017 The Ustore Authors.

#include "types/server/slist.h"

#include "node/list_node.h"
#include "node/node_builder.h"

namespace ustore {

SList::SList(std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer,
    const Hash& root_hash) noexcept : UList(loader), chunk_writer_(writer) {
  SetNodeForHash(root_hash);
}

SList::SList(std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer,
    const std::vector<Slice>& elements) noexcept
    : UList(loader), chunk_writer_(writer) {
  CHECK_GE(elements.size(), size_t(0));
  if (elements.size() == 0) {
    ChunkInfo chunk_info = ListChunker::Instance()->Make({});
    chunk_writer_->Write(chunk_info.chunk.hash(), chunk_info.chunk);
    SetNodeForHash(chunk_info.chunk.hash());
  } else {
    NodeBuilder nb(chunk_writer_, ListChunker::Instance(),
                   MetaChunker::Instance(), false);
    std::unique_ptr<const Segment> seg = ListNode::Encode(elements);
    nb.SpliceElements(0, seg.get());
    SetNodeForHash(nb.Commit());
  }
}

Hash SList::Splice(size_t start_idx, size_t num_to_delete,
                   const std::vector<Slice>& entries) const {
  CHECK(!empty());
  NodeBuilder nb(hash(), start_idx, chunk_loader_.get(), chunk_writer_,
                 ListChunker::Instance(), MetaChunker::Instance(), false);
  // TODO(pingcheng): can directly init a segment instance instead of unique_ptr
  //   only need to make SplicElements take const seqment& as parameter
  //   hance we can avoid `new` operation
  std::unique_ptr<const Segment> seg = ListNode::Encode({entries});
  nb.SpliceElements(num_to_delete, seg.get());
  return nb.Commit();
}

}  // namespace ustore
