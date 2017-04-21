// Copyright (c) 2017 The Ustore Authors.

#include "node/cursor.h"
#include "node/orderedkey.h"
#include "node/list_node.h"
#include "node/node_builder.h"
#include "types/ulist.h"
#include "utils/logging.h"

namespace ustore {

const Slice UList::Get(size_t idx) const {
  CHECK(!empty());
  auto cursor = std::unique_ptr<NodeCursor>
                    (NodeCursor::GetCursorByIndex(root_node_->hash(),
                    idx, chunk_loader_.get()));

  if (cursor->isEnd()) {
    return Slice(nullptr, 0);
  } else {
    return ListNode::decode(cursor->current());
  }
}

bool UList::SetNodeForHash(const Hash& root_hash) {
  const Chunk* chunk = chunk_loader_->Load(root_hash);
  if (chunk == nullptr) return false;

  if (chunk->type() == ChunkType::kMeta) {
    root_node_.reset(new MetaNode(chunk));
    return true;
  } else if (chunk->type() == ChunkType::kList) {
    root_node_.reset(new ListNode(chunk));
    return true;
  } else {
    LOG(FATAL) << "Cannot be other chunk type for UList.";
    return false;
  }
}

SList::SList(const Hash& root_hash) noexcept :
    UList(std::make_shared<ChunkLoader>()) {
  SetNodeForHash(root_hash);
}

SList::SList(const std::vector<Slice>& elements) noexcept:
    UList(std::make_shared<ChunkLoader>()) {
  CHECK_GT(elements.size(), 0);

  chunk_loader_ = std::move(std::make_shared<ChunkLoader>());

  NodeBuilder nb(ListChunker::Instance(), false);

  std::unique_ptr<const Segment> seg = ListNode::encode(elements);
  nb.SpliceElements(0, seg.get());
  SetNodeForHash(nb.Commit());
}

const Hash SList::Splice(size_t start_idx, size_t num_to_delete,
                         const std::vector<Slice>& entries) const {
  CHECK(!empty());
  NodeBuilder* nb = NodeBuilder::NewNodeBuilderAtIndex(hash(),
                                                       start_idx,
                                                       chunk_loader_.get(),
                                                       ListChunker::Instance(),
                                                       false);

  std::unique_ptr<const Segment> seg = ListNode::encode({entries});
  nb->SpliceElements(num_to_delete, seg.get());
  Hash root_hash = nb->Commit();
  delete nb;

  return root_hash;
}
}  // namespace ustore
