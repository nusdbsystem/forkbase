// Copyright (c) 2017 The Ustore Authors.

#include "node/cursor.h"
#include "node/orderedkey.h"
#include "node/node_comparator.h"
#include "types/umap.h"
#include "utils/logging.h"
#include "utils/debug.h"

namespace ustore {

std::unique_ptr<DuallyDiffKeyIterator> UMap::DuallyDiff(
    const UMap& lhs, const UMap& rhs) {
  std::unique_ptr<UIterator> lhs_diff_it = lhs.Diff(rhs);
  std::unique_ptr<UIterator> rhs_diff_it = rhs.Diff(lhs);

  lhs_diff_it->previous();
  rhs_diff_it->previous();

  DCHECK(lhs_diff_it->head() && rhs_diff_it->head());

  return std::unique_ptr<DuallyDiffKeyIterator>(
      new DuallyDiffKeyIterator(std::move(lhs_diff_it),
                                std::move(rhs_diff_it)));
}

Slice UMap::Get(const Slice& key) const {
  auto orderedkey = OrderedKey::FromSlice(key);
  NodeCursor* cursor = NodeCursor::GetCursorByKey(root_node_->hash(),
                         orderedkey, chunk_loader_.get());

  if (!cursor->isEnd() && orderedkey == cursor->currentKey()) {
    size_t value_size;
    auto value_data =
        reinterpret_cast<const char*>(MapNode::value(cursor->current(),
                                                     &value_size));
    return Slice(value_data, value_size);
  } else {
    return Slice(nullptr, 0);
  }
}

std::unique_ptr<UIterator> UMap::Scan() const {
  IndexRange all_range{0, numElements()};
  return std::unique_ptr<UIterator>(
      new MapIterator(hash(), {all_range}, chunk_loader_.get()));
}

std::unique_ptr<UIterator> UMap::Diff(const UMap& rhs) const {
  // Assume this and rhs both uses this chunk_loader_
  KeyComparator cmptor(rhs.hash(), chunk_loader_);
  return std::unique_ptr<UIterator>(
      new MapIterator(hash(), cmptor.Diff(hash()), chunk_loader_.get()));
}

std::unique_ptr<UIterator> UMap::Intersect(const UMap& rhs) const {
  // Assume this and rhs both uses this chunk_loader_
  KeyComparator cmptor(rhs.hash(), chunk_loader_);
  return std::unique_ptr<UIterator>(
      new MapIterator(hash(), cmptor.Intersect(hash()), chunk_loader_.get()));
}

bool UMap::SetNodeForHash(const Hash& root_hash) {
  const Chunk* chunk = chunk_loader_->Load(root_hash);
  if (chunk == nullptr) return false;
  if (chunk->type() == ChunkType::kMeta) {
    root_node_.reset(new MetaNode(chunk));
    return true;
  } else if (chunk->type() == ChunkType::kMap) {
    root_node_.reset(new MapNode(chunk));
    return true;
  } else {
    return false;
  }
}

}  // namespace ustore
