// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_NODE_BUILDER_H_
#define USTORE_NODE_NODE_BUILDER_H_

#include <list>
#include <memory>
#include <vector>

#include "chunk/chunker.h"
#include "chunk/chunk_loader.h"
#include "chunk/chunk_writer.h"
#include "node/cursor.h"
#include "node/rolling_hash.h"
#include "types/type.h"
#include "utils/noncopyable.h"

namespace ustore {
class NodeBuilder : private Noncopyable {
 public:
  // Perform operation at element with key at leaf rooted at root_hash
  NodeBuilder(const Hash& root_hash, const OrderedKey& key,
              ChunkLoader* chunk_loader, ChunkWriter* chunk_writer,
              const Chunker* chunker, bool isFixedEntryLen) noexcept;

  // Perform operation at idx-th element at leaf rooted at root_hash
  NodeBuilder(const Hash& root_hash, size_t idx,
              ChunkLoader* chunk_loader, ChunkWriter* chunk_writer,
              const Chunker* chunker, bool isFixedEntryLen) noexcept;

  // Construct a node builder to construct a fresh new Prolly Tree
  NodeBuilder(ChunkWriter* chunk_writer, const Chunker* chunker,
              bool isFixedEntryLen) noexcept;

  ~NodeBuilder() = default;

  // First delete num_delete elements from cursor and then
  // Append elements in Segment in byte array
  void SpliceElements(size_t num_delete, const Segment* element_seg);

  // Commit the uncommited operation
  // Create and dump the chunk into storage
  // @return The hash (a.k.a. the key) of the newly commited root chunk.
  Hash Commit();

 private:
  // Internal constructor used to recursively construct Parent NodeBuilder
  // is_leaf shall set to FALSE
  NodeBuilder(std::unique_ptr<NodeCursor>&& cursor,
              size_t level, ChunkWriter* chunk_writer, const Chunker* chunker,
              bool isFixedEntryLen) noexcept;

  NodeBuilder(size_t level, ChunkWriter* chunk_writer, const Chunker* chunker,
              bool isFixedEntryLen) noexcept;

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
  Chunk HandleBoundary(const std::vector<const Segment*>& segments);
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

  // Create an empty segment pointing data from current cursor
  //   This created segs are pushed to created_segs of this nodebuilder
  Segment* SegAtCursor();

  inline size_t numAppendSegs() const { return appended_segs_.size(); }
  // Access the parent builder.
  // Construct a new one, if not exists.
  NodeBuilder* parent_builder();

 private:
  std::unique_ptr<NodeCursor> cursor_;
  std::unique_ptr<NodeBuilder> parent_builder_;
  // a vector of appended segments for chunking
  std::vector<const Segment*> appended_segs_;
  // A vector to collect and own segs created by this nodebuilder
  std::vector<std::unique_ptr<const Segment>> created_segs_;

  Segment* pre_cursor_seg_;
  std::unique_ptr<RollingHasher> rhasher_;
  bool commited_ = true;    // false if exists operation to commit
  size_t num_skip_entries_ = 0;
  size_t level_ = 0;
  ChunkWriter* const chunk_writer_;
  const Chunker* const chunker_;
  // whether the built entry is fixed length
  // type blob: true
  const bool isFixedEntryLen_;
};

class AdvancedNodeBuilder : Noncopyable  {
/* A node builder that can support multiple operation in a single transaction
To construct a new prolly tree:
  AdvancedNodeBuilder nb;
  const Hash hash = nb.Insert(0, segment).Commit();
To work on an existing prolly tree:
  AdvancedNodeBuilder nb(root, loader);
  const Hash hash = nb.Insert(0, segment)
                      .Splice(1, 4, segment)
                      .Remove(4, 6)
                      .Commit();
*/
 public:
  AdvancedNodeBuilder(const Hash& root, ChunkLoader* loader_,
                      ChunkWriter* writer);

  // ctor to create a prolly tree from start
  explicit AdvancedNodeBuilder(ChunkWriter* writer);

  inline AdvancedNodeBuilder& Insert(uint64_t start_idx,
                                     const std::vector<const Segment*>& segs) {
    return Splice(start_idx, 0, segs);
  }

  inline AdvancedNodeBuilder& Insert(uint64_t start_idx,
                                     const Segment& seg) {
    return Splice(start_idx, 0, {&seg});
  }

  inline AdvancedNodeBuilder& Remove(uint64_t start_idx,
                                     uint64_t num_delete) {
    return Splice(start_idx, num_delete, {});
  }

  // segs can be empty.
  // NOTE: The segments in the argument list should be alive
  //   until commit is called.
  AdvancedNodeBuilder& Splice(uint64_t start_idx, uint64_t num_delete,
                              const std::vector<const Segment*>& segs);

  AdvancedNodeBuilder& Splice(uint64_t start_idx, uint64_t num_delete,
                              const Segment& seg) {
    return  Splice(start_idx, num_delete, {&seg});
  }

  Hash Commit(const Chunker& chunker, bool isFixedEntryLen);

 private:
  // relevant information to perform one spliced operation
  struct SpliceOperand {
    uint64_t start_idx;
    // the cursor points to the position for splicing
    std::unique_ptr<NodeCursor> cursor;
    // the number of entries to delete
    size_t num_delete;
    // a vector of segments to append
    // can be empty
    std::vector<const Segment*> appended_segs;
  };

  AdvancedNodeBuilder(size_t level, ChunkWriter* writer);

  AdvancedNodeBuilder(size_t level, const Hash& root,
                      ChunkLoader* loader_, ChunkWriter* writer);

  // return the parent builder,
  //   create one if not exists
  AdvancedNodeBuilder* parent();

  // Create the segment from chunk start until cursor (exclusive)
  // Place this segment into created_segs
  const Segment* InitPreCursorSeg(bool isFixedEntryLen,
                                  const NodeCursor& cursor);

  // Create an empty segment from the element pointed by the cursor
  // Place this segment into created_segs
  Segment* InitCursorSeg(bool isFixedEntryLen, const NodeCursor& cursor);

  // Make chunks based on the entries in the segments
  // Write the created chunk to chunk store
  // Reset the rolling hasher
  // return the created chunkinfo
  ChunkInfo HandleBoundary(const Chunker& chunker,
      const std::vector<const Segment*>& segments);

  // private members
  size_t level_;

  const Hash root_;
  ChunkLoader* loader_;
  ChunkWriter* writer_;

  std::unique_ptr<RollingHasher> rhasher_;
  std::unique_ptr<AdvancedNodeBuilder> parent_builder_;
  std::list<SpliceOperand> operands_;

  // A vector to collect and own segs created by this nodebuilder
  std::vector<std::unique_ptr<const Segment>> created_segs_;
  size_t num_created_entries_ = 0;
};
}  // namespace ustore

#endif  // USTORE_NODE_NODE_BUILDER_H_
