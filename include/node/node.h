// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_NODE_H_
#define USTORE_NODE_NODE_H_

#include <memory>
#include <vector>

#include "chunk/chunk.h"
#include "chunk/chunker.h"
#include "node/orderedkey.h"
#include "types/type.h"
#include "utils/singleton.h"

namespace ustore {
class UNode {
 public:
  explicit UNode(const Chunk* chunk) : chunk_(chunk) {}
  inline Hash hash() const { return chunk_->hash(); }

 protected:
  const Chunk* chunk_;
};

class SeqNode : public UNode {
  /* SeqNode represents a general node in Prolly Tree.

     Its subclass is either be a internal node containing meta-data
     Or it can be a leaf node containing blob chunk data
  */

 public:
  static std::unique_ptr<const SeqNode> CreateFromChunk(const Chunk* chunk);

  explicit SeqNode(const Chunk* chunk) : UNode(chunk) {}
  virtual ~SeqNode() {}  // NOT delete chunk!!

  // Whether this SeqNode is a leaf
  virtual bool isLeaf() const = 0;
  // Number of entries in this SeqNode
  // If this is MetaNode, return the number of containing MetaEntries
  // If this is a leaf, return the number of containing elements
  virtual size_t numEntries() const = 0;
  // number of elements at leaves rooted at this MetaSeq
  virtual uint64_t numElements() const = 0;

  // return the byte pointer for the idx-th entry in this node
  virtual const byte_t* data(size_t idx) const = 0;

  virtual OrderedKey key(size_t idx) const = 0;
  // return the byte len of the idx-th entry
  virtual size_t len(size_t idx) const = 0;
};

class MetaNode : public SeqNode {
  /* MetaNode is a non-leaf node in prolly tree.

    It consists of multiple MetaEntries.

    Encoding Scheme by MetaNode:
    |-num_entries-|-----MetaEntry 1----|-----MetaEntry 2----|
    0------------ 4 ----------variable size
  */
 public:
  explicit MetaNode(const Chunk* chunk) : SeqNode(chunk) { PrecomputeOffset(); }
  ~MetaNode() override {}

  inline bool isLeaf() const override { return false; }
  size_t numEntries() const override;
  uint64_t numElements() const override;

  // the total number of elements from the first
  // to the entryidx-th entry (exclusive)
  // Caller of this method has to make sure entry_idx is valid.
  uint64_t numElementsUntilEntry(size_t entry_idx) const;

  // return the byte pointer for the idx-th entry in this node
  const byte_t* data(size_t idx) const override;

  OrderedKey key(size_t idx) const override;

  // return the byte len of the idx-th entry
  size_t len(size_t idx) const override;

  // Retreive the ChildHash in the MetaEntry
  // which contains the idx-th element rooted at this metanode
  // Return empty hash and entry_idx=numEntries if such MetaEntry not exist.
  Hash GetChildHashByIndex(size_t element_idx, size_t* entry_idx) const;

  // Retreive the ChildHash in the entry_idx-th MetaEntry
  // Caller of this method has to make sure entry_idx is valid.
  Hash GetChildHashByEntry(size_t entry_idx) const;

  // Retreive the child hash pointed by the MetaEntry,
  // The Ordered Key of this MetaEntry
  // has the smallest OrderedKey that is no smaller than the compared key
  // Return empty hash and entry_idx=numEntries if such MetaEntry not exist.
  Hash GetChildHashByKey(const OrderedKey& key, size_t* entry_idx) const;

 private:
  size_t entryOffset(size_t idx) const;

  // compute the offset for all entries and store the offsets
  //   in a class member vector
  void PrecomputeOffset();

  std::vector<size_t> offsets_;
};

class MetaChunker : public Singleton<MetaChunker>, public Chunker {
  friend class Singleton<MetaChunker>;

 public:
  ChunkInfo Make(const std::vector<const Segment*>& segments) const
      override;

 private:
  MetaChunker() {}
  ~MetaChunker() {}
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

  explicit MetaEntry(const byte_t* data) : data_(data) {}
  ~MetaEntry() {}  // do nothing

  OrderedKey orderedKey() const;

  // num of bytes in MetaEntry
  size_t numBytes() const;
  // num of leaves rooted at this MetaEntry
  uint32_t numLeaves() const;
  // num of elements at all leaves rooted at this MetaEntry
  uint64_t numElements() const;

  Hash targetHash() const;

 private:
  static constexpr size_t kNumBytesOffset = 0;
  static constexpr size_t kNumLeavesOffset = kNumBytesOffset + sizeof(uint32_t);
  static constexpr size_t kNumElementsOffset =
      kNumLeavesOffset + sizeof(uint32_t);
  static constexpr size_t kHashOffset = kNumElementsOffset + sizeof(uint64_t);
  static constexpr size_t kKeyOffset = kHashOffset + Hash::kByteLength;

  const byte_t* data_;  // MetaEntry is NOT responsible to clear
};

class LeafNode : public SeqNode {
  /* LeafNode is a leaf node in prolly tree.
   * It is a abstract leaf node for Blob/List/Set/etc...
  */
 public:
  explicit LeafNode(const Chunk* chunk) : SeqNode(chunk) {}
  ~LeafNode() override {}

  inline uint64_t numElements() const { return numEntries(); }

  inline bool isLeaf() const override { return true; }
  // Get #bytes from start-th element (inclusive) to end-th element (exclusive)
  virtual size_t GetLength(size_t start, size_t end) const = 0;
  // Copy num_bytes bytes from start-th element (inclusive)
  // Buffer capacity shall be large enough.
  // return the number of bytes actually read
  virtual size_t Copy(size_t start, size_t num_bytes, byte_t* buffer) const = 0;

  // get the idx of element with the smallest key
  //   no smaller than the parameter key
  // If key of all elements < parameter key,
  //  return the number of entries.
  virtual size_t GetIdxForKey(const OrderedKey& key) const = 0;
};

}  // namespace ustore
#endif  // USTORE_NODE_NODE_H_
