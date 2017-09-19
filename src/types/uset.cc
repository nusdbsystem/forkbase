// Copyright (c) 2017 The Ustore Authors.

#include "node/cursor.h"
#include "node/orderedkey.h"
#include "node/node_comparator.h"
#include "types/uset.h"
#include "utils/logging.h"
#include "utils/debug.h"

namespace ustore {

DuallyDiffKeyIterator USet::DuallyDiff(
  const USet& lhs, const USet& rhs) {
  std::unique_ptr<CursorIterator> lhs_diff_it(
    new USet::Iterator(lhs.Diff(rhs)));
  std::unique_ptr<CursorIterator> rhs_diff_it(
    new USet::Iterator(rhs.Diff(lhs)));

  lhs_diff_it->previous();
  rhs_diff_it->previous();

  DCHECK(lhs_diff_it->head() && rhs_diff_it->head());
  return DuallyDiffKeyIterator(std::move(lhs_diff_it), std::move(rhs_diff_it));
}

Slice USet::Get(const Slice& key) const {
  auto orderedkey = OrderedKey::FromSlice(key);
  NodeCursor cursor(root_node_->hash(), orderedkey, chunk_loader_.get());

  if (!cursor.isEnd() && orderedkey == cursor.currentKey()) {
    return key;
  } else {
    return Slice();
  }
}

USet::Iterator USet::Scan() const {
  if (numElements() == 0) {
    return USet::Iterator(hash(), {}, chunk_loader_.get());
  } else {
    IndexRange all_range{0, numElements()};
    return USet::Iterator(hash(), {all_range}, chunk_loader_.get());
  }
}

USet::Iterator USet::Diff(const USet& rhs) const {
  // Assume this and rhs both uses this chunk_loader_
  if (this->numElements() == 0) {
    return USet::Iterator(hash(), {}, chunk_loader_.get());
  } else if (rhs.numElements() == 0) {
    return USet::Iterator(hash(), {{0, numElements()}}, chunk_loader_.get());
  } else {
    KeyComparator cmptor(rhs.hash(), chunk_loader_);
    return USet::Iterator(hash(), cmptor.Diff(hash()), chunk_loader_.get());
  }
}

USet::Iterator USet::Intersect(const USet& rhs) const {
  // Assume this and rhs both uses this chunk_loader_
  if (this->numElements() == 0 || rhs.numElements() == 0) {
    return USet::Iterator(hash(), {}, chunk_loader_.get());
  } else {
    KeyComparator cmptor(rhs.hash(), chunk_loader_);
    return USet::Iterator(hash(), cmptor.Intersect(hash()),
                          chunk_loader_.get());
  }
}

bool USet::SetNodeForHash(const Hash& root_hash) {
  const Chunk* chunk = chunk_loader_->Load(root_hash);
  if (chunk == nullptr) return false;
  if (chunk->type() == ChunkType::kMeta) {
    root_node_.reset(new MetaNode(chunk));
    return true;
  } else if (chunk->type() == ChunkType::kSet) {
    root_node_.reset(new SetNode(chunk));
    return true;
  } else {
    return false;
  }
}

std::ostream& operator<<(std::ostream& os, const USet& obj) {
  auto it = obj.Scan();
  auto f_print_it = [&os, &it]() {
    os << "(" << it.key() << ")";
  };
  os << "[";
  if (!it.end()) {
    f_print_it();
    for (it.next(); !it.end(); it.next()) {
      os << ", ";
      f_print_it();
    }
  }
  os << "]";
  return os;
}

}  // namespace ustore
