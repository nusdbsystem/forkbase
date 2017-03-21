// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_UBLOB_H_
#define USTORE_TYPES_UBLOB_H_

#include <cstddef>
#include "chunk/chunk.h"
#include "hash/hash.h"
#include "node/chunk_loader.h"
#include "node/tree_node.h"
#include "types/type.h"
#include "utils/noncopyable.h"

namespace ustore {
class UBlob : private Noncopyable {
 public:
  // Create Chunk Loader
  // Get Chunk c
  // if c.type ==  MetaNode
  //    MetaNode(t)
  // else if:
  //    BlobLeafnode(t)
  // else
  //   Panic()
  static UBlob Load(const Hash& root_hash);
  // Create the new UBlob from initial data
  // Create the ChunkLoader
  static UBlob Create(const byte_t* data, size_t num_bytes);

  ~UBlob();  // remove ChunkLoader

  // Return the number of bytes in this Blob
  inline size_t size() const { return root_node_->numElements(); }

  /** Delete some bytes from a position and insert new bytes
   *
   *  Args:
   *    pos: the byte position to remove or insert bytes
   *    num_delete: the number of bytes to be deleted
   *    data: the byte array to insert after deletion
   *    num_insert: number of bytes in array to be inserted into current blob
   *
   *  Return:
   *    the new Blob reflecting the operation
   */
  const UBlob Splice(size_t pos, size_t num_delete, byte_t* data,
                     size_t num_insert) const;
  /** Insert bytes given a position
   *
   *  Use Slice internally
   */
  const UBlob Insert(size_t pos, const byte_t* data, size_t num_insert) const;
  /** Delete bytes from a given position
   *
   *  Use Slice internally
   */
  const UBlob Delete(size_t pos, size_t num_delete) const;
  /** Append bytes from the last position of Blob
   *
   *  Use Slice internally
   */
  const UBlob Append(byte_t* data, size_t num_insert) const;
  /** Read the blob data and copy into buffer
   *    Args:
   *      pos: the number of position to read
   *      len: the number of subsequent bytes to read into buffer
   *      buffer: the byte array from which the data is copied to
   *
   *    Return:
   *      the number of bytes that actually read
   */
  size_t Read(size_t pos, size_t len, byte_t* buffer) const;

 private:
  // Private contrucstor to create an instance based on the root chunk data
  // To be called by Load() and Init()
  explicit UBlob(const Chunk* chunk);

  // Can either be a leaf(BlobLeafNode) or a non-leaf (MetaNode)
  // Responsible to remove
  const SeqNode* root_node_;

  ChunkLoader* chunk_loader_;  // responsible to delete
};

}  // namespace ustore
#endif  // USTORE_TYPES_UBLOB_H_
