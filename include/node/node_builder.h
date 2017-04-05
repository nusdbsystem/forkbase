// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_NODE_BUILDER_H_
#define USTORE_NODE_NODE_BUILDER_H_

#include <cstddef>
#include <vector>
#include <utility>

#include "chunk/chunk.h"
#include "chunk/chunker.h"
#include "node/cursor.h"
#include "node/rolling_hash.h"
#include "store/chunk_loader.h"
#include "types/type.h"

namespace ustore {
class NodeBuilder {
 public:
  // TODO(wangji): if a chunker is binded to only fixed or unfixed length,
  //               it is better to move isFixedEntryLen inside chunker impl
  // Perform operation at idx-th element at leaf rooted at root_hash
  static NodeBuilder* NewNodeBuilderAtIndex(const Hash& root_hash, size_t idx,
                                            ChunkLoader* chunk_loader,
                                            const Chunker* chunker,
                                            bool isFixedEntryLen);
  // Perform operation at idx-th element at leaf rooted at root_hash
  static NodeBuilder* NewNodeBuilderAtKey(const Hash& root_hash,
                                          const OrderedKey& key,
                                          ChunkLoader* chunk_loader,
                                          const Chunker* chunker,
                                          bool isFixedEntryLen);
  // Construct a node builder to construct a fresh new Prolly Tree
  explicit NodeBuilder(const Chunker* chunker, bool isFixedEntryLen);

  ~NodeBuilder();

  // First delete num_delete elements from cursor and then
  // Append elements in Segment in byte array
  void SpliceElements(size_t num_delete, const Segment* element_seg);

  // Commit the uncommited operation
  // Create and dump the chunk into storage
  //  nullptr returned if fail to dump
  const Chunk* Commit();

 private:
  // Internal constructor used to recursively construct Parent NodeBuilder
  // is_leaf shall set to FALSE
  NodeBuilder(NodeCursor* cursor, size_t level, const Chunker* chunker,
              bool isFixedEntryLen);

  NodeBuilder(size_t level, const Chunker* chunker, bool isFixedEntryLen);
  // Remove elements from cursor
  // Return the number of elements actually removed
  size_t SkipEntries(size_t num_elements);

  // Append entries in a segment
  void AppendSegmentEntries(const Segment* entry_seg);

  // Make chunks based on the entries in the segments
  // Pass the created metaentry(a segment with a single entry)
  //   to upper builders to append
  // reset the rolling hasher
  // return the created chunk
  const Chunk* HandleBoundary(const std::vector<const Segment*>& segments);
  // Two things to do:
  //  * Populate the rolling hash with preceding elements before cursor point
  //      until its window size filled up
  //  * Populate the buffer with data from SeqNode head until cursor point
  //  NOTE: Be sure NOT to rolling hash num_entries bytes at MetaNode head
  void Resume();

  // Whether a node builder will build a single-entry MetaNode
  //   This node is invalid and shall be excluded from final built tree
  inline bool isInvalidNode() const {
    return cursor_ == nullptr && numAppendSegs() <= 1;
  }

  // create an empty segment pointing data from current cursor
  Segment* SegAtCursor() const;

  inline size_t numAppendSegs() const { return appended_segs_.size(); }
  // Access the parent builder.
  // Construct a new one, if not exists.
  NodeBuilder* parent_builder();

 private:
  NodeCursor* cursor_;  // shall be deleted during destruction
  NodeBuilder* parent_builder_;  // shall be deleted during destruction
  // a vector of appended segments for chunking
  std::vector<const Segment*> appended_segs_;
  Segment* pre_cursor_seg_;  // shall be deleted during destruction
  RollingHasher* rhasher_;  // shall be deleted
  bool commited_ = true;  // false if exists operation to commit
  size_t num_skip_entries_ = 0;
  size_t level_ = 0;
  const Chunker* chunker_;
  // whether the built entry is fixed length
  // type blob: true
  const bool isFixedEntryLen_;
};
}  // namespace ustore

#endif  // USTORE_NODE_NODE_BUILDER_H_
