// Copyright (c) 2017 The Ustore Authors.

#include "node/cursor.h"
#include "node/node_comparator.h"
#include "node/orderedkey.h"
#include "node/list_node.h"
#include "node/node_builder.h"
#include "types/ulist.h"
#include "utils/logging.h"

namespace ustore {
std::unique_ptr<DuallyDiffIndexIterator> UList::DuallyDiff(
    const UList& lhs, const UList& rhs) {
  std::unique_ptr<UIterator> lhs_diff_it = lhs.Diff(rhs);
  std::unique_ptr<UIterator> rhs_diff_it = rhs.Diff(lhs);

  lhs_diff_it->previous();
  rhs_diff_it->previous();

  DCHECK(lhs_diff_it->head() && rhs_diff_it->head());

  return std::unique_ptr<DuallyDiffIndexIterator>(
      new DuallyDiffIndexIterator(std::move(lhs_diff_it),
                                  std::move(rhs_diff_it)));
}

Slice UList::Get(uint64_t idx) const {
  CHECK(!empty());
  auto cursor = std::unique_ptr<NodeCursor>
                    (NodeCursor::GetCursorByIndex(root_node_->hash(),
                    idx, chunk_loader_.get()));

  if (cursor == nullptr || cursor->isEnd()) {
    return Slice(nullptr, 0);
  } else {
    return ListNode::Decode(cursor->current());
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

Hash UList::Delete(uint64_t start_idx,
                   uint64_t num_to_delete) const {
  return Splice(start_idx, num_to_delete, {});
}

Hash UList::Insert(uint64_t start_idx,
                   const std::vector<Slice>& entries) const {
  return Splice(start_idx, 0, entries);
}

Hash UList::Append(const std::vector<Slice>& entries) const {
  return Splice(numElements(), 0, entries);
}

std::unique_ptr<UIterator> UList::Scan() const {
  IndexRange all_range{0, numElements()};

  return std::unique_ptr<UIterator>(
      new ListIterator(hash(), {all_range}, chunk_loader_.get()));
}

std::unique_ptr<UIterator> UList::Diff(const UList& rhs) const {
  // Assume this and rhs both uses this chunk_loader_
  IndexComparator cmptor(rhs.hash(), chunk_loader_);

  return std::unique_ptr<UIterator>(
      new ListIterator(hash(), cmptor.Diff(hash()), chunk_loader_.get()));
}

std::unique_ptr<UIterator> UList::Intersect(const UList& rhs) const {
  // Assume this and rhs both uses this chunk_loader_
  IndexComparator cmptor(rhs.hash(), chunk_loader_);

  return std::unique_ptr<UIterator>(
      new ListIterator(hash(), cmptor.Intersect(hash()), chunk_loader_.get()));
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

  std::unique_ptr<const Segment> seg = ListNode::Encode(elements);
  nb.SpliceElements(0, seg.get());
  SetNodeForHash(nb.Commit());
}

Hash SList::Splice(size_t start_idx, size_t num_to_delete,
                         const std::vector<Slice>& entries) const {
  CHECK(!empty());
  NodeBuilder* nb = NodeBuilder::NewNodeBuilderAtIndex(hash(),
                                                       start_idx,
                                                       chunk_loader_.get(),
                                                       ListChunker::Instance(),
                                                       false);

  std::unique_ptr<const Segment> seg = ListNode::Encode({entries});
  nb->SpliceElements(num_to_delete, seg.get());
  Hash root_hash = nb->Commit();
  delete nb;

  return root_hash;
}
}  // namespace ustore
