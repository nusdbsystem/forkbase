// Copyright (c) 2017 The Ustore Authors.

#include "node/cursor.h"
#include "node/orderedkey.h"
#include "node/node_merger.h"
#include "types/umap.h"
#include "utils/debug.h"
#include "utils/logging.h"

namespace ustore {

const size_t UMap::kHashByteLength = Hash::kByteLength;

DuallyDiffKeyIterator UMap::DuallyDiff(
  const UMap& lhs, const UMap& rhs) {
  std::unique_ptr<CursorIterator> lhs_diff_it(
    new UMap::Iterator(lhs.Diff(rhs)));
  std::unique_ptr<CursorIterator> rhs_diff_it(
    new UMap::Iterator(rhs.Diff(lhs)));

  lhs_diff_it->previous();
  rhs_diff_it->previous();

  DCHECK(lhs_diff_it->head() && rhs_diff_it->head());
  return DuallyDiffKeyIterator(std::move(lhs_diff_it), std::move(rhs_diff_it));
}

Slice UMap::Get(const Slice& key) const {
  auto orderedkey = OrderedKey::FromSlice(key);
  NodeCursor cursor(root_node_->hash(), orderedkey, chunk_loader_.get());

  if (!cursor.isEnd() && orderedkey == cursor.currentKey()) {
    size_t value_size;
    auto value_data = MapNode::value(cursor.current(), &value_size);
    return Slice(value_data, value_size);
  } else {
    return Slice();
  }
}

UMap::Iterator UMap::Scan() const {
  if (numElements() == 0) {
    return UMap::Iterator(hash(), {}, chunk_loader_.get());
  } else {
    IndexRange all_range{0, numElements()};
    return UMap::Iterator(hash(), {all_range}, chunk_loader_.get());
  }
}

UMap::Iterator UMap::Diff(const UMap& rhs) const {
  // Assume this and rhs both uses this chunk_loader_
  if (this->numElements() == 0) {
    return UMap::Iterator(hash(), {}, chunk_loader_.get());
  } else if (rhs.numElements() == 0) {
    return UMap::Iterator(hash(), {{0, numElements()}}, chunk_loader_.get());
  } else {
    KeyDiffer differ(rhs.hash(), rhs.chunk_loader_.get());
    std::vector<IndexRange> ranges = differ.Compare(hash(),
                                     chunk_loader_.get());
    return UMap::Iterator(hash(), ranges, chunk_loader_.get());
  }
}

UMap::Iterator UMap::Intersect(const UMap& rhs) const {
  // Assume this and rhs both uses this chunk_loader_
  if (this->numElements() == 0 || rhs.numElements() == 0) {
    return UMap::Iterator(hash(), {}, chunk_loader_.get());
  } else {
    KeyIntersector intersector(rhs.hash(), rhs.chunk_loader_.get());
    std::vector<IndexRange> ranges = intersector.Compare(hash(),
                                     chunk_loader_.get());
    return UMap::Iterator(hash(), ranges, chunk_loader_.get());
  }
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

std::ostream& operator<<(std::ostream& os, const UMap& obj) {
  auto it = obj.Scan();
  auto f_print_it = [&os, &it]() {
    os << "(" << it.key() << "->" << it.value() << ")";
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
