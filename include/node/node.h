// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_NODE_H_
#define USTORE_NODE_NODE_H_

#include <cstddef>
#include <utility>
#include <vector>

#include "chunk/chunk.h"
#include "node/orderedkey.h"
#include "types/type.h"

namespace ustore {

// a customized MakeChunkFunc shall return a pair struct
//   the first points to the created chunk
//   the second is also a pair struct
//      the first points to metanetry byte array
//      the second is the number of bytes in this byte array
typedef std::pair<const Chunk*, std::pair<const byte_t*, size_t> > ChunkInfo;

typedef const ChunkInfo (*MakeChunkFunc)(
                     const std::vector<const byte_t*>& entries_data,
                     const std::vector<size_t>& entries_num_bytes);

class SeqNode {
  /* SeqNode represents a general node in Prolly Tree.

     Its subclass is either be a internal node containing meta-data
     Or it can be a leaf node containing blob chunk data
  */

 public:
  explicit SeqNode(const Chunk* chunk);
  virtual ~SeqNode();  // NOT delete chunk!!

  // Whether this SeqNode is a leaf
  virtual bool isLeaf() const = 0;
  // Number of entries in this SeqNode
  // If this is MetaNode, return the number of containing MetaEntries
  // If this is a leaf, return the number of containing elements
  virtual size_t numEntries() const = 0;
  // number of elements at leaves rooted at this MetaSeq
  virtual uint64_t numElements() const = 0;

  // return the byte pointer for the idx-th element in leaf
  virtual const byte_t* data(size_t idx) const = 0;

  // return the byte len of the idx-th entry
  virtual size_t len(size_t idx) const = 0;

  inline const Hash hash() const { return chunk_->hash();}

 protected:
  const Chunk* chunk_;
};

class MetaNode : public SeqNode {
  /* MetaNode is a non-leaf node in prolly tree.

    It consists of multiple MetaEntries.

    Encoding Scheme by MetaNode:
    |-num_entries-|-----MetaEntry 1----|-----MetaEntry 2----|
    0------------ 4 ----------variable size
  */
 public:
  //  Create junk from encoded MetaEntry bytes and their byte number
  static const ChunkInfo MakeChunk
                          (const std::vector<const byte_t*>& entries_data,
                           const std::vector<size_t>& entries_num_bytes);

  explicit MetaNode(const Chunk* chunk);
  ~MetaNode() override;

  inline bool isLeaf() const override { return false; }
  size_t numEntries() const override;
  uint64_t numElements() const override;

  // the total number of elements from the first
  // to the entryidx-th entry (exclusive)
  // Caller of this method has to make sure entry_idx is valid.
  uint64_t numElementsUntilEntry(size_t entry_idx) const;

  // return the byte pointer for the idx-th element in leaf
  const byte_t* data(size_t idx) const override;
  // return the byte len of the idx-th entry
  size_t len(size_t idx) const override;

  // Retreive the ChildHash in the MetaEntry
  // which contains the idx-th element rooted at this metanode
  // Return empty hash and entry_idx=numEntries if such MetaEntry not exist.
  const Hash GetChildHashByIndex(size_t element_idx, size_t* entry_idx) const;

  // Retreive the ChildHash in the entry_idx-th MetaEntry
  // Caller of this method has to make sure entry_idx is valid.
  const Hash GetChildHashByEntry(size_t entry_idx) const;

  // Retreive the child hash pointed by the MetaEntry,
  // The Ordered Key of this MetaEntry
  // has the smallest OrderedKey that is no smaller than the compared key
  // Return empty hash and entry_idx=numEntries if such MetaEntry not exist.
  const Hash GetChildHashByKey(const OrderedKey& key, size_t* entry_idx) const;

 private:
  size_t entryOffset(size_t idx) const;
};

class MetaEntry {
  /* MetaEntry points a child MetaNode/LeafNode

    Encoding Scheme by MetaEntry (variable size)
    |-num_bytes-|-num_leaves-|-num_elements-|-data hash-|--Ordered Key---|
    0-----------4 -----------8--------------16----------36-variable size-|
  */
 public:
  // encode a MetaEntry into formated byte array
  // given relevant parameters.
  // @args [out] num_of_bytes encoded.
  static const byte_t* Encode(uint32_t num_leaves, uint64_t num_elements,
                              const Hash& data_hash, const OrderedKey& key,
                              size_t* encode_num_bytes);

  explicit MetaEntry(const byte_t* data);
  ~MetaEntry();  // do nothing

  const OrderedKey orderedKey() const;

  // num of bytes in MetaEntry
  size_t numBytes() const;
  // num of leaves rooted at this MetaEntry
  uint32_t numLeaves() const;
  // num of elements at all leaves rooted at this MetaEntry
  uint64_t numElements() const;
  const Hash targetHash() const;

 private:
  const byte_t* data_;  // MetaEntry is NOT responsible to clear

  static const size_t NUM_BYTE_OFFSET = 0;
  static const size_t NUM_LEAF_OFFSET = NUM_BYTE_OFFSET + sizeof(uint32_t);
  static const size_t NUM_ELEMENT_OFFSET = NUM_LEAF_OFFSET + sizeof(uint32_t);
  static const size_t HASH_OFFSET = NUM_ELEMENT_OFFSET + sizeof(uint64_t);
  static const size_t KEY_OFFSET = HASH_OFFSET + HASH_BYTE_LEN;
};

class LeafNode : public SeqNode {
  /* LeafNode is a leaf node in prolly tree.
   * It is a abstract leaf node for Blob/List/Set/etc...
  */
 public:
  explicit LeafNode(const Chunk* chunk);

  virtual ~LeafNode();

  inline uint64_t numElements() const { return numEntries();}

  inline bool isLeaf() const override { return true; }
  // Get #bytes from start-th element (inclusive) to end-th element (exclusive)
  virtual size_t GetLength(size_t start, size_t end) const = 0;
  // Copy num_bytes bytes from start-th element (inclusive)
  // Buffer capacity shall be large enough.
  // return the number of bytes actually read
  virtual size_t Copy(size_t start, size_t num_bytes, byte_t* buffer) const = 0;

  // get the idx of element with the smallest key
  //   no smaller than the parameter key
  // found = true if they are equal, false otherwise
  // If key of all elements < parameter key,
  //  return the number of entries.
  virtual size_t GetIdxForKey(const OrderedKey& key,
                                    bool* found) const = 0;
};

}  // namespace ustore
#endif  // USTORE_NODE_NODE_H_