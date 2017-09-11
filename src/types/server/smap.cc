// Copyright (c) 2017 The Ustore Authors.

#include "types/server/smap.h"

#include "node/map_node.h"
#include "node/node_builder.h"
#include "utils/debug.h"
#include "utils/utils.h"

namespace ustore {
SMap::SMap(const Hash& root_hash, std::shared_ptr<ChunkLoader> loader,
    ChunkWriter* writer) noexcept : UMap(loader), chunk_writer_(writer) {
  SetNodeForHash(root_hash);
}

SMap::SMap(const std::vector<Slice>& keys, const std::vector<Slice>& vals,
    std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer) noexcept
    : UMap(loader), chunk_writer_(writer) {
  CHECK_GE(keys.size(), size_t(0));
  CHECK_EQ(vals.size(), keys.size());
  if (keys.size() == 0) {
    ChunkInfo chunk_info = MapChunker::Instance()->Make({});
    chunk_writer_->Write(chunk_info.chunk.hash(), chunk_info.chunk);
    SetNodeForHash(chunk_info.chunk.hash());
  } else {
    NodeBuilder nb(chunk_writer_, MapChunker::Instance(), false);
    std::vector<KVItem> kv_items;

    for (size_t i : Utils::SortIndexes<Slice>(keys)) {
      kv_items.push_back({keys[i], vals[i]});
    }
    std::unique_ptr<const Segment> seg = MapNode::Encode(kv_items);
    nb.SpliceElements(0, seg.get());
    SetNodeForHash(nb.Commit());
  }
}

Hash SMap::Set(const Slice& key, const Slice& val) const {
  CHECK(!empty());
  const OrderedKey orderedKey = OrderedKey::FromSlice(key);
  NodeBuilder nb(hash(), orderedKey, chunk_loader_.get(),
                 chunk_writer_, MapChunker::Instance(), false);

  // Try to find whether this key already exists
  NodeCursor cursor(hash(), orderedKey, chunk_loader_.get());
  bool foundKey = (!cursor.isEnd() && orderedKey == cursor.currentKey());
  size_t num_splice = foundKey? 1: 0;
  // If the item with identical key exists,
  //   remove it to replace
  KVItem kv_item = {key, val};

  std::unique_ptr<const Segment> seg = MapNode::Encode({kv_item});
  nb.SpliceElements(num_splice, seg.get());
  return nb.Commit();
}

Hash SMap::Remove(const Slice& key) const {
  CHECK(!empty());
  const OrderedKey orderedKey = OrderedKey::FromSlice(key);

  // Use cursor to find whether that key exists
  NodeCursor cursor(hash(), orderedKey, chunk_loader_.get());
  bool foundKey = (!cursor.isEnd() && orderedKey == cursor.currentKey());

  if (!foundKey) {
    LOG(WARNING) << "Try to remove a non-existent key "
                 << "(" << byte2str(orderedKey.data(),
                                    orderedKey.numBytes()) << ")"
                 << " From Map " << hash().ToBase32();
    return hash();
  }

  // Create an empty segment
  VarSegment seg(std::unique_ptr<const byte_t[]>(nullptr), 0, {});
  NodeBuilder nb(hash(), orderedKey, chunk_loader_.get(),
                 chunk_writer_, MapChunker::Instance(), false);
  nb.SpliceElements(1, &seg);
  return nb.Commit();
}

}  // namespace ustore
