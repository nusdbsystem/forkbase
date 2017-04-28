// Copyright (c) 2017 The Ustore Authors.

#include "node/cursor.h"
#include "node/orderedkey.h"
#include "node/map_node.h"
#include "node/node_builder.h"
#include "types/umap.h"
#include "utils/logging.h"

namespace ustore {

KVIterator::KVIterator(std::unique_ptr<NodeCursor> cursor):
    Iterator(std::move(cursor)) {}

const Slice KVIterator::key() const {
  CHECK(!end());
  size_t len;
  const char* data =  reinterpret_cast<const char*>(
                          MapNode::key(cursor_->current(), &len));
  return Slice(data, len);
}

const Slice KVIterator::value() const {
  CHECK(!end());
  size_t len;

  const char* data =  reinterpret_cast<const char*>(
                          MapNode::value(cursor_->current(), &len));
  return Slice(data, len);
}

const Slice UMap::Get(const Slice& key) const {
  bool foundKey = false;

  auto orderedkey = OrderedKey::FromSlice(key);

  NodeCursor* cursor = NodeCursor::GetCursorByKey(root_node_->hash(),
                                                  orderedkey,
                                                  chunk_loader_.get(),
                                                  &foundKey);

  if (foundKey) {
    size_t value_size;
    auto value_data =
        reinterpret_cast<const char*>(MapNode::value(cursor->current(),
                                                     &value_size));
    return Slice(value_data, value_size);
  } else {
    return Slice(nullptr, 0);
  }
}

std::unique_ptr<KVIterator> UMap::iterator() const {
  CHECK(!empty());
  std::unique_ptr<NodeCursor> cursor(
    NodeCursor::GetCursorByIndex(root_node_->hash(),
                                 0, chunk_loader_.get()));

  return std::unique_ptr<KVIterator>(new KVIterator(std::move(cursor)));
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

const Hash SMap::Set(const Slice& key, const Slice& val) const {
  CHECK(!empty());

  bool foundKey = false;
  NodeBuilder* nb = NodeBuilder::NewNodeBuilderAtKey(hash(),
                                                     OrderedKey::FromSlice(key),
                                                     chunk_loader_.get(),
                                                     MapChunker::Instance(),
                                                     false,
                                                     &foundKey);
  // If the item with identical key exists,
  //   remove it to replace
  KVItem kv_item = {reinterpret_cast<const byte_t*>(key.data()),
                   reinterpret_cast<const byte_t*>(val.data()),
                   key.len(),
                   val.len()};

  size_t num_splice = foundKey? 1: 0;
  std::unique_ptr<const Segment> seg = MapNode::Encode({kv_item});
  nb->SpliceElements(num_splice, seg.get());
  Hash root_hash = nb->Commit();
  delete nb;

  return root_hash;
}

const Hash SMap::Remove(const Slice& key) const {
  CHECK(!empty());
  // Use cursor to find whether that key exists
  bool foundKey = false;

  // Create an empty segment
  VarSegment seg(std::unique_ptr<const byte_t[]>(nullptr),
                 0, {});

  NodeBuilder* nb = NodeBuilder::NewNodeBuilderAtKey(hash(),
                                                     OrderedKey::FromSlice(key),
                                                     chunk_loader_.get(),
                                                     MapChunker::Instance(),
                                                     false,
                                                     &foundKey);
  if (foundKey) {
    nb->SpliceElements(1, &seg);
    Hash hash = nb->Commit();
    delete nb;
    return hash;
  }

  // return self hash if not found
  return hash();
}
}  // namespace ustore
