// Copyright (c) 2017 The Ustore Authors.
#include <cstddef>

#include "chunk/chunk.h"
#include "types/type.h"
#include "node/tree_node.h"

#ifndef USTORE_TYPES_NODE_BLOB_NODE_H_
#define USTORE_TYPES_NODE_BLOB_NODE_H_

namespace ustore {
class BlobNode: public LeafNode {
/*
BlobNode is a leaf node in Prolly tree that contains
actual blob data

Encoding Scheme:
  | ------blob bytes ------|
  | ------variable size
*/
 public:
  explicit BlobNode(const Chunk* chunk);
  ~BlobNode() override;

  inline size_t numEntries() const override { return this->capacity(); }
  inline uint64_t numElements() const override { return this->capacity(); }
  inline size_t entryOffset(size_t idx) const override { return idx; }
  inline size_t GetLength(size_t start,
                          size_t end) const override {
                                              return end - start;
                                            }

  size_t Copy(size_t start, size_t num_bytes, byte_t* buffer) const override;
};
}  // namespace ustore
#endif  // USTORE_TYPES_NODE_BLOB_NODE_H_
