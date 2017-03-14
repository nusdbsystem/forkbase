// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_BLOB_NODE_H_
#define USTORE_NODE_BLOB_NODE_H_

#include <cstddef>
#include <vector>

#include "chunk/chunk.h"
#include "node/node.h"

namespace ustore {

class BlobNode : public LeafNode {
/*
BlobNode is a leaf node in Prolly tree that contains
actual blob data

Encoding Scheme:
  | ------blob bytes ------|
  | ------variable size
*/
 public:
  static const ChunkInfo MakeChunk
                              (const std::vector<const byte_t*>& element_data,
                               const std::vector<size_t>& element_num_bytes);

  explicit BlobNode(const Chunk* chunk);
  ~BlobNode();

  const byte_t* data(size_t idx) const override { return chunk_->data() + idx;}
  // return the byte len of the idx-th entry
  inline size_t len(size_t idx) const override { return 1;}

  inline size_t numEntries() const override { return chunk_->capacity(); }
  inline size_t GetLength(size_t start, size_t end) const override {
    return end - start;
  }
  size_t Copy(size_t start, size_t num_bytes, byte_t* buffer) const override;

  size_t GetIdxForKey(const OrderedKey& key, bool* found) const override;
};

}  // namespace ustore

#endif  // USTORE_NODE_BLOB_NODE_H_
