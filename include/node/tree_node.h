// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_TREE_NODE_H_
#define USTORE_TYPES_TREE_NODE_H_

#include <cstddef>

#include "chunk/chunk.h"
#include "node/orderedkey.h"
#include "types/type.h"

namespace ustore {
class SeqNode {
/* SeqNode represents a general node in Prolly Tree.

   Its subclass is either be a internal node containing meta-data
   Or it can be a leaf node containing blob chunk data
*/

 public:
  explicit SeqNode(const Chunk* chunk);
  virtual ~SeqNode() = 0;  // NOT delete chunk!!

  inline Type type() const { return chunk_->type(); }
  inline size_t capacity() const { return chunk_->capacity(); }
  inline const Chunk* chunk() const { return chunk_; }

  // Whether this SeqNode is a leaf
  virtual bool isLeaf() const = 0;
  // Numbre of entreis in this SeqNode
  // If this is MetaNode, return the number of containing MetaEntries
  // If this is a leaf, return the number of containing elements
  virtual size_t numEntries() const = 0;
  // number of elements at leaves rooted at this MetaSeq
  virtual uint64_t numElements() const = 0;
  // Return the byte offset of the idx-th entry
  // relative to this chunk data pointer
  virtual size_t entryOffset(size_t idx)  const = 0;

 private:
  const Chunk* chunk_;
};

class MetaNode: public SeqNode {
/* MetaNode is a non-leaf node in prolly tree.

  It consists of multiple MetaEntries.

  Encoding Scheme by MetaNode:
  |-num_entries-|-----MetaEntry 1----|-----MetaEntry 2----|
  0------------ 4 ----------variable size
*/
 public:
  explicit MetaNode(const Chunk* chunk);
  ~MetaNode() override;

  inline bool isLeaf() const override { return false; }
  size_t numEntries() const override;
  uint64_t numElements() const override;
  size_t entryOffset(size_t idx) const override;

  // Retreive the SeqNode pointed by idx-th MetaEntry in this MetaNode
  const SeqNode* GetSeqNodeByIndex(size_t idx) const;

  // Retreive the SeqNode pointed by the MetaEntry,
  // The Ordered Key of this MetaEntry
  // has the smallest OrderedKey that is no smaller than the compared key
  const SeqNode* GetSeqNodeByKey(const OrderedKey& key) const;
};

class MetaEntry {
/* MetaEntry points a hild MetaNode

  Encoding Scheme by MetaEntry (variable size)
  |--num_bytes--|-num_leaves--|-----num_elements----|----- Ordered Key---|
  0 ----------- 4 ----------- 8 ------------------- 16 ---variable size
*/
 public:
  MetaEntry(const byte_t* data, size_t num_bytes);

  const OrderedKey* ordered_key() const;

  // num of leaves rooted at this MetaEntry
  inline uint32_t numLeaves() const;
  // num of elements at all leaves rooted at this MetaEntry
  inline uint64_t numElements() const;

 private:
  const byte_t* data_;  // MetaEntry is NOT responsible to clear
  size_t num_bytes_;  // num of bytes of data array
};

class LeafNode: public SeqNode {
/* LeafNode is a leaf node in prolly tree.
 * It is a abstract leaf node for Blob/List/Set/etc...
*/
 public:
  inline bool isLeaf() const override { return true; }

  // Get #bytes from start-th element (inclusive) to end-th element (exclusive)
  virtual size_t GetLength(size_t start, size_t end) const = 0;

  // Copy num_bytes bytes from start-th element (inclusive)
  // Buffer capacity shall be large enough.
  // return the number of bytes actually read
  virtual size_t Copy(size_t start, size_t num_bytes, byte_t* buffer) const = 0;
};
}  // namespace ustore
#endif  // USTORE_TYPES_TREE_NODE_H_
