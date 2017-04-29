// Copyright (c) 2017 The Ustore Authors.

#include "node/cursor.h"
#include "node/orderedkey.h"
#include "node/node_builder.h"
#include "node/node_comparator.h"
#include "types/umap.h"
#include "utils/logging.h"
#include "utils/debug.h"

namespace ustore {

Slice UMap::Get(const Slice& key) const {
  auto orderedkey = OrderedKey::FromSlice(key);

  NodeCursor* cursor = NodeCursor::GetCursorByKey(root_node_->hash(),
                                                  orderedkey,
                                                  chunk_loader_.get());

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

SMap::SMap(const Hash& root_hash) noexcept :
    UMap(std::make_shared<ChunkLoader>()) {
  SetNodeForHash(root_hash);
}

SMap::SMap(const std::vector<Slice>& keys,
           const std::vector<Slice>& vals) noexcept :
    UMap(std::make_shared<ChunkLoader>()) {
  CHECK_GT(keys.size(), 0);
  CHECK_EQ(vals.size(), keys.size());

  NodeBuilder nb(MapChunker::Instance(), false);

  std::vector<KVItem> kv_items;
  for (size_t i = 0; i < keys.size(); i++) {
    KVItem item = {reinterpret_cast<const byte_t*>(keys[i].data()),
                   reinterpret_cast<const byte_t*>(vals[i].data()),
                   keys[i].len(),
                   vals[i].len()};

    kv_items.push_back(item);
  }

  std::unique_ptr<const Segment> seg = MapNode::Encode(kv_items);
  nb.SpliceElements(0, seg.get());
  SetNodeForHash(nb.Commit());
}

Hash SMap::Set(const Slice& key, const Slice& val) const {
  CHECK(!empty());
  const OrderedKey orderedKey = OrderedKey::FromSlice(key);
  NodeBuilder* nb = NodeBuilder::NewNodeBuilderAtKey(hash(),
                                                     orderedKey,
                                                     chunk_loader_.get(),
                                                     MapChunker::Instance(),
                                                     false);


  // Try to find whether this key already exists
  NodeCursor *cursor = NodeCursor::GetCursorByKey(hash(),
                                                  orderedKey,
                                                  chunk_loader_.get());

  bool foundKey = (!cursor->isEnd() && orderedKey == cursor->currentKey());

  delete cursor;
  size_t num_splice = foundKey? 1: 0;

  // If the item with identical key exists,
  //   remove it to replace
  KVItem kv_item = {reinterpret_cast<const byte_t*>(key.data()),
                   reinterpret_cast<const byte_t*>(val.data()),
                   key.len(),
                   val.len()};


  std::unique_ptr<const Segment> seg = MapNode::Encode({kv_item});
  nb->SpliceElements(num_splice, seg.get());
  Hash root_hash = nb->Commit();
  delete nb;

  return root_hash;
}

Hash SMap::Remove(const Slice& key) const {
  CHECK(!empty());
  const OrderedKey orderedKey = OrderedKey::FromSlice(key);

  // Use cursor to find whether that key exists
  NodeCursor *cursor = NodeCursor::GetCursorByKey(hash(),
                                                  orderedKey,
                                                  chunk_loader_.get());

  bool foundKey = (!cursor->isEnd() && orderedKey == cursor->currentKey());

  delete cursor;

  if (!foundKey) {
    LOG(WARNING) << "Try to remove a non-existent key "
                 << "(" << byte2str(orderedKey.data(),
                                    orderedKey.numBytes()) << ")"
                 << " From Map " << hash().ToBase32();
    return hash();
  }


  // Create an empty segment
  VarSegment seg(std::unique_ptr<const byte_t[]>(nullptr),
                 0, {});

  NodeBuilder* nb = NodeBuilder::NewNodeBuilderAtKey(hash(),
                                                     orderedKey,
                                                     chunk_loader_.get(),
                                                     MapChunker::Instance(),
                                                     false);

  nb->SpliceElements(1, &seg);
  Hash hash = nb->Commit();
  delete nb;
  return hash;

}
}  // namespace ustore
