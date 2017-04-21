// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_LIST_NODE_H_
#define USTORE_NODE_LIST_NODE_H_

#include <vector>

#include "node/node.h"

namespace ustore {
struct KVItem;  // forward declare of KVItem in umap.cc

class ListChunker : public Singleton<ListChunker>, public Chunker {
 friend class Singleton<ListChunker>;

 public:
  ChunkInfo make(const std::vector<const Segment*>& segments) const
      override;

 private:
  ListChunker() = default;
  ~ListChunker() = default;
};

class ListNode : public LeafNode {
// Encoding scheme for mapnode
// | # of entries- | ---entry 1--|--entry 2--| ---
// 0 ------------- 4 --var size--|--var size--| ---

// Entry Encoding:
// | # of bytes (including this) | ----entry data---- |
// | ----------------------------4 ----var size ----- |
 public:
  // Make segments from multiple slice elements
  static std::unique_ptr<const Segment> encode(
      const std::vector<Slice>& elements);

  static const Slice decode(const byte_t* data);

  explicit ListNode(const Chunk* chunk) : LeafNode(chunk) {
    PrecomputeOffsets();
  }
  ~ListNode() override {}

  const byte_t* data(size_t idx) const override;
  // return the byte len of the idx-th entry
  size_t len(size_t idx) const override;

  size_t numEntries() const override;

  // ListNode doesnot implement this API
  size_t Copy(size_t start, size_t num_bytes, byte_t* buffer) const override;
  size_t GetLength(size_t start, size_t end) const override;
  size_t GetIdxForKey(const OrderedKey& key, bool* found) const override;

 private:
  void PrecomputeOffsets();
  // a vector byte offset relative to chunk data
  std::vector<size_t> offsets_;
};

}  // namespace ustore

#endif  // USTORE_NODE_LIST_NODE_H_
