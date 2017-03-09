// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_NODEBUILDER_H_
#define USTORE_TYPES_NODEBUILDER_H_

#include <cstddef>
#include <vector>

#include "types/type.h"
#include "types/node.h"
#include "types/cursor.h"
#include "types/rolling_hash.h"
#include "chunk/chunk.h"

namespace ustore {

class NodeBuilder {
 public:
  // Perform operation at idx-th element at leaf rooted at root_hash
  NodeBuilder(const Hash& root_hash, size_t idx);

  // Construct a node builder to construct a fresh new Prolly Tree
  static NodeBuilder EmptyNodeBuilder();

  // Remove elements from cursor
  // Return the number of elements actually removed
  size_t SkipElements(size_t num_elements);

  // Append the bytes of a single element to NodeBuilder
  //   Return whether the appending succeeds
  // NOTE: data will be deleted after this call
  bool AppendElement(const byte* data, size_t num_bytes);

  // Return the number of bytes actually appended
  //   This method will treat each byte as a single element
  // NOTE: data will be deleted after this call
  bool AppendBytes(const byte* data, size_t num_bytes);

  // Commit the uncommited operation
  // Create and dump the chunk into storage
  //  nullptr returned if fail to dump
  const Chunk* Commit();

 private:
  // Internal constructor used to recursively construct Parent NodeBuilder
  // is_leaf shall set to FALSE
  NodeBuilder(const Hash& root_hash, size_t idx, NodeBuilder* parent_builder);

  // Two things to do:
  //  * Populate the rolling hash with preceding elements before cursor point
  //      until its window size filled up
  //  * Populate the buffer with data from SeqNode head until cursor point
  //  NOTE: Be sure NOT to rolling hash num_entries bytes at MetaNode head
  void Resume();

  // Skip one element at Parent Chunker
  // return the operation succeeds or not
  bool SkipParentIfExists();

  // Create the parent node builder
  NodeBuilder* CreateParentNodeBuilder();


// Private Members
  NodeCursor* cursor_;  // shall be deleted during destruction

  NodeBuilder* parent_builder_;  // shall be deleted during destruction

  bool is_leaf;  // whether this NodeBuilder works on leaf node

  // raw byte of a list of elements to append
  std::vector<const byte*> append_data_list_;

  // a list of element raw byte size
  std::vector<const size_t> append_data_size_;

  // The pointer to SeqNode data, SHALL NOT DELETE during destruction
  //    It points to the data to be appended in the new chunk
  //    Therefore, for MetaNode,
  //    it points the starting bytes of the first MetaEntry
  const byte* node_head_;

  // Number of bytes pointed by node_head_ to append
  const size_t num_append_bytes;

  // Number of elements to append
  const size_t num_elements;

  RollingHasher rhasher;
};
}  // namespace ustore

#endif  // USTORE_TYPES_NODEBUILDER_H
