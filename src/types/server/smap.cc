// Copyright (c) 2017 The Ustore Authors.

#include "types/server/smap.h"

#include "node/map_node.h"
#include "node/node_builder.h"
#include "node/node_comparator.h"
#include "node/node_merger.h"
#include "utils/debug.h"
#include "utils/utils.h"

namespace ustore {
SMap::SMap(std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer,
    const Hash& root_hash) noexcept : UMap(loader), chunk_writer_(writer) {
  SetNodeForHash(root_hash);
}

SMap::SMap(std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer,
    const std::vector<Slice>& keys, const std::vector<Slice>& vals) noexcept
    : UMap(loader), chunk_writer_(writer) {
  CHECK_GE(keys.size(), size_t(0));
  CHECK_EQ(vals.size(), keys.size());
  if (keys.size() == 0) {
    ChunkInfo chunk_info = MapChunker::Instance()->Make({});
    chunk_writer_->Write(chunk_info.chunk.hash(), chunk_info.chunk);
    SetNodeForHash(chunk_info.chunk.hash());
  } else {
    NodeBuilder nb(chunk_writer_, MapChunker::Instance(),
                   MetaChunker::Instance());
    std::vector<KVItem> kv_items;

    auto sorted_idx = Utils::SortIndexes<Slice>(keys);
    for (auto sorted_idx_it = sorted_idx.begin();
         sorted_idx_it != sorted_idx.end();
         ++sorted_idx_it) {
      auto next_it = sorted_idx_it + 1;
      if (next_it == sorted_idx.end()
          || keys[*sorted_idx_it] < keys[*next_it]) {
        kv_items.push_back({keys[*sorted_idx_it], vals[*sorted_idx_it]});
      } else if (keys[*sorted_idx_it] == keys[*next_it]) {
        // do nothing, skip this duplicated key
      } else {
        LOG(FATAL) << "Error in sorting keys.";
      }  // end if
    }  // end for
    std::unique_ptr<const Segment> seg = MapNode::Encode(kv_items);
    nb.SpliceElements(0, seg.get());
    SetNodeForHash(nb.Commit());
  }
}

Hash SMap::Set(const Slice& key, const Slice& val) const {
  CHECK(!empty());
  const OrderedKey orderedKey = OrderedKey::FromSlice(key);
  NodeBuilder nb(hash(), orderedKey, chunk_loader_.get(),
                 chunk_writer_, MapChunker::Instance(),
                 MetaChunker::Instance());

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

Hash SMap::Set(const std::vector<Slice>& keys,
               const std::vector<Slice>& vals) const {
  CHECK(!empty());
  AdvancedNodeBuilder nb(hash(), chunk_loader_.get(), chunk_writer_);

  auto sorted_idx = Utils::SortIndexes<Slice>(keys);
  for (auto sorted_idx_it = sorted_idx.begin();
       sorted_idx_it != sorted_idx.end();
       ++sorted_idx_it) {
    auto next_it = sorted_idx_it + 1;
    if (next_it == sorted_idx.end() || keys[*sorted_idx_it] < keys[*next_it]) {
      size_t idx = *sorted_idx_it;
      OrderedKey orderKey = OrderedKey::FromSlice(keys[idx]);
      NodeCursor cursor(hash(), orderKey, chunk_loader_.get());
      bool foundKey = (!cursor.isEnd() && orderKey == cursor.currentKey());
      size_t num_delete = foundKey? 1: 0;

      uint64_t idxForKey =
          root_node_->FindIndexForKey(orderKey, chunk_loader_.get());

      KVItem kv_item = {keys[idx], vals[idx]};
      std::unique_ptr<const Segment> seg = MapNode::Encode({kv_item});
      std::vector<std::unique_ptr<const Segment>> segs;
      segs.push_back(std::move(seg));
      nb.Splice(idxForKey, num_delete, std::move(segs));

    } else if (keys[*sorted_idx_it] == keys[*next_it]) {
      // do nothing, skip this duplicated key
    } else {
      LOG(FATAL) << "Error in sorting keys.";
    }  // end if
  }  // end for
  return nb.Commit(*MapChunker::Instance());
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
                 chunk_writer_, MapChunker::Instance(),
                 MetaChunker::Instance());
  nb.SpliceElements(1, &seg);
  return nb.Commit();
}

Hash SMap::Merge(const SMap& node1, const SMap& node2) const {
  if (numElements() == 0 || node1.numElements() == 0 ||
      node2.numElements() == 0) {
    // Merge fails if an empty map exists.
    return Hash();
  }

  KeyMerger merger(hash(), chunk_loader_.get(), chunk_writer_);
  return merger.Merge(node1.hash(), node2.hash(),
                      *MapChunker::Instance());
}

}  // namespace ustore
