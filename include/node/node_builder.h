// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_NODE_BUILDER_H_
#define USTORE_NODE_NODE_BUILDER_H_

#include <cstddef>
#include <deque>
#include <vector>

#include "chunk/chunk.h"
#include "node/chunk_loader.h"
#include "node/cursor.h"
#include "node/rolling_hash.h"
#include "types/type.h"

namespace ustore {

class NodeBuilder {
 public:
  // Perform operation at idx-th element at leaf rooted at root_hash
  static NodeBuilder* NewNodeBuilderAtIndex(const Hash& root_hash, size_t idx,
                                            ChunkLoader* chunk_loader);
  // Perform operation at idx-th element at leaf rooted at root_hash
  static NodeBuilder* NewNodeBuilderAtKey(const Hash& root_hash,
                                          const OrderedKey& key,
                                          ChunkLoader* chunk_loader);

  // Construct a node builder to construct a fresh new Prolly Tree
  NodeBuilder();
  ~NodeBuilder();

  // First delete num_delete elements from cursor and then
  // Append elements in byte array
  // NOTE: Byte arrays in element_data will be deleted after use.
  void SpliceEntries(size_t num_delete,
                      const std::vector<const byte_t*>& element_data,
                      const std::vector<size_t>& num_bytes);


  // Commit the uncommited operation
  // Create and dump the chunk into storage
  //  nullptr returned if fail to dump
  const Chunk* Commit(MakeChunkFunc make_chunk);


 private:
  // Internal constructor used to recursively construct Parent NodeBuilder
  // is_leaf shall set to FALSE
  explicit NodeBuilder(NodeCursor* cursor, size_t level);

  explicit NodeBuilder(size_t level);
  // Remove elements from cursor
  // Return the number of elements actually removed
  size_t SkipEntries(size_t num_elements);

  // Append the bytes of a single entry to NodeBuilder
  void AppendEntry(const byte_t* data, size_t num_bytes);

  // Make chunks based on given entries data
  // Pass the created metaentry to upper builders to append
  // reset the rolling hasher
  // clear the parameter array (NOTE: Byte arrays in element_data will be
  // deleted after use).
  // return the created chunk
  const Chunk* HandleBoundary(std::vector<const byte_t*>* chunk_entries_data,
                              std::vector<size_t>* chunk_entries_num_bytes,
                              MakeChunkFunc make_chunk);
  // Two things to do:
  //  * Populate the rolling hash with preceding elements before cursor point
  //      until its window size filled up
  //  * Populate the buffer with data from SeqNode head until cursor point
  //  NOTE: Be sure NOT to rolling hash num_entries bytes at MetaNode head
  void Resume();

  inline bool isCommited() const { return commited_;}

  // Whether a node builder will build a single-entry MetaNode
  //   This node is invalid and shall be excluded from final tree
  inline bool isInvalidNode() const {
    return cursor_ == nullptr && num_appended_entries_ <= 1;
  }

  size_t numAppend() const { return num_appended_entries_; }
  // Access the parent builder.
  // Construct a new one, if not exists.
  NodeBuilder* parent_builder();
  // Private Members
  NodeCursor* cursor_;  // shall be deleted during destruction
  NodeBuilder* parent_builder_;  // shall be deleted during destruction

  // raw byte of a list of entries to append
  // use deque because we both need to push front and push end
  std::deque<const byte_t*> entries_data_;
  // a list of entry raw byte size
  std::deque<size_t> entries_num_bytes_;

  RollingHasher* rhasher_;  // shall be deleted
  bool commited_ = true;  // false if exists operation to commit

  // number of entries appended from lower level
  size_t num_appended_entries_;

  size_t num_skip_entries_ = 0;

  size_t level_ = 0;
};
}  // namespace ustore

#endif  // USTORE_NODE_NODE_BUILDER_H_
