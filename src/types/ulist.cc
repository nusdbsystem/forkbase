// Copyright (c) 2017 The Ustore Authors.

#include "types/ulist.h"

#include <memory>
#include <utility>
#include "node/cursor.h"
#include "node/node_comparator.h"
#include "node/orderedkey.h"
#include "utils/logging.h"

namespace ustore {

DuallyDiffIndexIterator UList::DuallyDiff(const UList& lhs, const UList& rhs) {
  std::unique_ptr<CursorIterator> lhs_diff_it(
    new UList::Iterator(lhs.Diff(rhs)));
  std::unique_ptr<CursorIterator> rhs_diff_it(
    new UList::Iterator(rhs.Diff(lhs)));

  lhs_diff_it->previous();
  rhs_diff_it->previous();

  DCHECK(lhs_diff_it->head() && rhs_diff_it->head());
  return DuallyDiffIndexIterator(std::move(lhs_diff_it),
                                 std::move(rhs_diff_it));
}

Slice UList::Get(uint64_t idx) const {
  CHECK(!empty());
  NodeCursor cursor(root_node_->hash(), idx, chunk_loader_.get());

  if (cursor.empty() || cursor.isEnd())
    return Slice();
  else
    return ListNode::Decode(cursor.current());
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

Hash UList::Delete(uint64_t start_idx, uint64_t num_to_delete) const {
  return Splice(start_idx, num_to_delete, {});
}

Hash UList::Insert(uint64_t start_idx,
                   const std::vector<Slice>& entries) const {
  return Splice(start_idx, 0, entries);
}

Hash UList::Append(const std::vector<Slice>& entries) const {
  return Splice(numElements(), 0, entries);
}

UList::Iterator UList::Scan() const {
  if (numElements() == 0) {
    return Iterator(hash(), {}, chunk_loader_.get());
  } else {
    IndexRange all_range{0, numElements()};
    return Iterator(hash(), {all_range}, chunk_loader_.get());
  }
}

UList::Iterator UList::Diff(const UList& rhs) const {
  // Assume this and rhs both uses this chunk_loader_
  if (this->numElements() == 0) {
    return UList::Iterator(hash(), {}, chunk_loader_.get());
  } else if (rhs.numElements() == 0) {
    return UList::Iterator(hash(), {{0, numElements()}}, chunk_loader_.get());
  } else {
    IndexDiffer differ(rhs.hash(), rhs.chunk_loader_.get());
    auto ranges = differ.Compare(hash(), chunk_loader_.get());
    return UList::Iterator(hash(), ranges, chunk_loader_.get());
  }
}

UList::Iterator UList::Intersect(const UList& rhs) const {
  // Assume this and rhs both uses this chunk_loader_

  if (this->numElements() == 0 || rhs.numElements() == 0) {
    return UList::Iterator(hash(), {}, chunk_loader_.get());
  } else {
    IndexIntersector intersector(rhs.hash(), rhs.chunk_loader_.get());
    auto ranges = intersector.Compare(hash(), chunk_loader_.get());
    return UList::Iterator(hash(), ranges, chunk_loader_.get());
  }
}

std::ostream& operator<<(std::ostream& os, const UList& obj) {
  auto it = obj.Scan();
  os << "[";
  if (!it.end()) {
    os << it.value();
    for (it.next(); !it.end(); it.next()) {
      os << ", " << it.value();
    }
  }
  os << "]";
  return os;
}

}  // namespace ustore
