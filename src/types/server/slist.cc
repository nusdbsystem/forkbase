// Copyright (c) 2017 The Ustore Authors.

#include "types/server/slist.h"

#include "node/list_node.h"
#include "node/node_builder.h"

namespace ustore {

SList::SList(const Hash& root_hash) noexcept :
    UList(std::make_shared<ServerChunkLoader>()) {
  SetNodeForHash(root_hash);
}

SList::SList(const std::vector<Slice>& elements) noexcept:
    UList(std::make_shared<ServerChunkLoader>()) {
  CHECK_GE(elements.size(), 0);
  if (elements.size() == 0) {
    ChunkInfo chunk_info = ListChunker::Instance()->Make({});
    store::GetChunkStore()->Put(chunk_info.chunk.hash(), chunk_info.chunk);
    SetNodeForHash(chunk_info.chunk.hash());
  } else {
    NodeBuilder nb(ListChunker::Instance(), false);
    std::unique_ptr<const Segment> seg = ListNode::Encode(elements);
    nb.SpliceElements(0, seg.get());
    SetNodeForHash(nb.Commit());
  }
}

Hash SList::Splice(size_t start_idx, size_t num_to_delete,
                   const std::vector<Slice>& entries) const {
  CHECK(!empty());
  NodeBuilder* nb = NodeBuilder::NewNodeBuilderAtIndex(hash(), start_idx,
    chunk_loader_.get(), ListChunker::Instance(), false);
  std::unique_ptr<const Segment> seg = ListNode::Encode({entries});
  nb->SpliceElements(num_to_delete, seg.get());
  Hash root_hash = nb->Commit();
  delete nb;

  return root_hash;
}

}  // namespace ustore
