// Copyright (c) 2017 The Ustore Authors.

#include "types/server/sset.h"

#include "node/set_node.h"
#include "node/node_builder.h"
#include "utils/debug.h"
#include "utils/utils.h"

namespace ustore {
SSet::SSet(std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer,
    const Hash& root_hash) noexcept : USet(loader), chunk_writer_(writer) {
  SetNodeForHash(root_hash);
}

SSet::SSet(std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer,
    const std::vector<Slice>& keys) noexcept
    : USet(loader), chunk_writer_(writer) {
  CHECK_GE(keys.size(), size_t(0));
  if (keys.size() == 0) {
    ChunkInfo chunk_info = SetChunker::Instance()->Make({});
    chunk_writer_->Write(chunk_info.chunk.hash(), chunk_info.chunk);
    SetNodeForHash(chunk_info.chunk.hash());
  } else {
    NodeBuilder nb(chunk_writer_, SetChunker::Instance(), false);
    std::vector<Slice> items;

    for (size_t i : Utils::SortIndexes<Slice>(keys)) {
      items.push_back(keys[i]);
    }
    std::unique_ptr<const Segment> seg = SetNode::Encode(items);
    nb.SpliceElements(0, seg.get());
    SetNodeForHash(nb.Commit());
  }
}

Hash SSet::Set(const Slice& key) const {
  CHECK(!empty());
  const OrderedKey orderedKey = OrderedKey::FromSlice(key);
  NodeBuilder nb(hash(), orderedKey, chunk_loader_.get(),
                 chunk_writer_, SetChunker::Instance(), false);

  // Try to find whether this key already exists
  NodeCursor cursor(hash(), orderedKey, chunk_loader_.get());
  bool foundKey = (!cursor.isEnd() && orderedKey == cursor.currentKey());

  if (foundKey) {
    DLOG(WARNING) << "Try to add a existent key "
                  << "(" << byte2str(orderedKey.data(),
                                     orderedKey.numBytes()) << ")"
                  << " From Set " << hash().ToBase32();
    return hash();
  }

  std::unique_ptr<const Segment> seg = SetNode::Encode({key});
  nb.SpliceElements(0, seg.get());
  return nb.Commit();
}

Hash SSet::Remove(const Slice& key) const {
  CHECK(!empty());
  const OrderedKey orderedKey = OrderedKey::FromSlice(key);

  // Use cursor to find whether that key exists
  NodeCursor cursor(hash(), orderedKey, chunk_loader_.get());
  bool foundKey = (!cursor.isEnd() && orderedKey == cursor.currentKey());

  if (!foundKey) {
    LOG(WARNING) << "Try to remove a non-existent key "
                 << "(" << byte2str(orderedKey.data(),
                                    orderedKey.numBytes()) << ")"
                 << " From Set " << hash().ToBase32();
    return hash();
  }

  // Create an empty segment
  VarSegment seg(std::unique_ptr<const byte_t[]>(nullptr), 0, {});
  NodeBuilder nb(hash(), orderedKey, chunk_loader_.get(),
                 chunk_writer_, SetChunker::Instance(), false);
  nb.SpliceElements(1, &seg);
  return nb.Commit();
}

}  // namespace ustore
