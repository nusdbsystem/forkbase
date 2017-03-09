// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_NODE_H_
#define USTORE_TYPES_NODE_H_

#include <cstddef>

#include "types/type.h"
#include "types/orderedkey.h"
#include "chunk/chunk.h"

namespace ustore {
class SeqNode {
/* SeqNode represents a general node in Prolly Tree.

   Its subclass is either be a internal node containing meta-data
   Or it can be a leaf node containing blob chunk data
*/

 public:
  explicit SeqNode(const Chunk* chunk);

  virtual ~SeqNode() = 0;  // delete the internal chunk

  inline Type type() const {return chunk_->type();}

  inline size_t capacity() const {return chunk_->capacity();}

  inline const* Chunk chunk() const { return chunk_;}

  // Whether this SeqNode is a leaf
  virtual bool is_leaf() const = 0;

  // Numbre of entreis in this SeqNode
  // If this is MetaNode, return the number of containing MetaEntries
  // If this is a leaf, return the number of containing elements
  virtual size_t num_entries() const = 0;

  // number of elements at leaves rooted at this MetaSeq
  virtual uint64_t num_elements() const = 0;

  // Return the byte offset of the idx-th entry
  // relative to this chunk data pointer
  virtual size_t entry_offset(size_t idx)  const = 0;

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

  inline bool is_leaf() override const { return false;}

  // number of MetaEntries in this MetaSeq
  size_t num_entries() override const;

  // number of elements rooted at this MetaNode
  uint64_t num_elements() override const;

  size_t entry_offset(size_t idx) override const;

  // Retreive the SeqNode pointed by idx-th MetaEntry in this MetaNode
  const SeqNode* GetSeqNodeByIndex(size_t idx) const;

  // Retreive the SeqNode pointed by the MetaEntry,
  // The Ordered Key of this MetaEntry
  // has the smallest OrderedKey that is no smaller than the compared key
  const SeqNode* GetSeqNodeForKey(const OrderedKey& key) const;
};

class MetaEntry {
/* MetaEntry points a hild MetaNode

  Encoding Scheme by MetaEntry (variable size)
  |--num_bytes--|-num_leaves--|-----num_elements----|----- Ordered Key---|
  0 ----------- 4 ----------- 8 ------------------- 16 ---variable size

*/
 public:
  MetaEntry(const byte* data, size_t num_bytes);

  const OrderedKey* ordered_key() const;

  // num of leaves rooted at this MetaEntry
  uint32_t num_leaves() const;

  // num of elements at all leaves rooted at this MetaEntry
  uint64_t num_elements() const;

 private:
  const byte* data_;  // MetaEntry is NOT responsible to clear

  size_t num_bytes;  // num of bytes of data array

};

class BlobLeafNode: public SeqNode {
/*
BlobLeafNode is a leaf node in Prolly tree that contains
actual blob data

Encoding Scheme:
  | ------blob bytes ------|
  | ------variable size
*/
 public:
  explicit BlobLeafNode(const Chunk* chunk);

  ~BlobLeafNode() override;

  inline bool is_leaf() override const { return true;}

  inline size_t num_entries() override const { return this->capacity();}

  inline uint64_t num_elements() override const {return this->capacity();}

  inline size_t entry_offset(size_t idx) override const { return idx;}

  // Copy the bytes from start position (inclusive)
  //  and end position (exclusive) towards buffer
  // Buffer capacity shall be larger then end - start
  // return the number of bytes actually read
  size_t Copy(size_t start, size_t end, byte* buffer) const;
};

class StringNode {
/* StringNode contains a single string.

  Encoding Scheme:
  | -str_len- | ------string bytes ------|
  | --------- 4 ------variable size

*/
 public:
  explicit StringNode(const Chunk* chunk);

  ~StringNode();

  size_t len() const;  // the byte count of this string

// Copy all the string bytes to buffer
// Buffer capacity shall be larger than this string len
// return the number of bytes copied.
  size_t Copy(byte* buffer) const;

 private:
  const Chunk* chunk_;
};

class CellNode {
/* CellNode contains a UCell

  Encoding Scheme:
  | -pre_ucell_hash_value- | -data-root-hash_value-- | --type--|
  | --------- -- --------- 20 -----------------------40 ------ 41
*/
 public:
  explicit CellNode(const Chunk* chunk);

  ~CellNode();

  const byte* GetPrevHashValue() const;

  const byte* GetDataHashValue() const;

  Type type() const;

 private:
  const Chunk* chunk_;
};
}  // namespace ustore
#endif  // USTORE_TYPES_NODE_H_
