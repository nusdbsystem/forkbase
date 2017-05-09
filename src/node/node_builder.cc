// Copyright (c) 2017 The Ustore Authors.

#include "node/node_builder.h"

#include <cstring>  // for memcpy
#include "hash/hash.h"
#include "node/node.h"
#include "node/orderedkey.h"
#include "store/chunk_store.h"

#include "utils/debug.h"
#include "utils/logging.h"

namespace ustore {

NodeBuilder* NodeBuilder::NewNodeBuilderAtIndex(const Hash& root_hash,
                                                size_t idx,
                                                ChunkLoader* chunk_loader,
                                                const Chunker* chunker,
                                                bool isFixedEntryLen) {
  NodeCursor* cursor =
      NodeCursor::GetCursorByIndex(root_hash, idx, chunk_loader);
  NodeBuilder* builder = new NodeBuilder(cursor, 0, chunker, isFixedEntryLen);
  return builder;
}

NodeBuilder* NodeBuilder::NewNodeBuilderAtKey(const Hash& root_hash,
                                              const OrderedKey& key,
                                              ChunkLoader* chunk_loader,
                                              const Chunker* chunker,
                                              bool isFixedEntryLen) {
  NodeCursor* cursor = NodeCursor::GetCursorByKey(root_hash, key,
                                                  chunk_loader);
  // cursor now points to the leaf node
  NodeBuilder* builder = new NodeBuilder(cursor, 0, chunker, isFixedEntryLen);
  return builder;
}

NodeBuilder::NodeBuilder(size_t level, const Chunker* chunker,
                         bool isFixedEntryLen)
    : cursor_(nullptr),
      parent_builder_(nullptr),
      appended_segs_(),
      created_segs_(),
      pre_cursor_seg_(nullptr),
#ifdef TEST_NODEBUILDER
      rhasher_(RollingHasher::TestHasher()),
#else
      rhasher_(new RollingHasher()),
#endif
      commited_(true),
      num_skip_entries_(0),
      level_(level),
      chunker_(chunker),
      isFixedEntryLen_(isFixedEntryLen) {
  // do nothing
}

NodeBuilder::NodeBuilder(const Chunker* chunker, bool isFixedEntryLen)
    : NodeBuilder(0, chunker, isFixedEntryLen) {}

NodeBuilder::~NodeBuilder() {
  delete rhasher_;
  delete cursor_;
  delete parent_builder_;
}

NodeBuilder::NodeBuilder(NodeCursor* cursor, size_t level,
                         const Chunker* chunker, bool isFixedEntryLen)
    : cursor_(new NodeCursor(*cursor)),
      parent_builder_(nullptr),
      appended_segs_(),
      created_segs_(),
      pre_cursor_seg_(nullptr),
#ifdef TEST_NODEBUILDER
      rhasher_(RollingHasher::TestHasher()),
#else
      rhasher_(new RollingHasher()),
#endif
      commited_(true),
      num_skip_entries_(0),
      level_(level),
      chunker_(chunker),
      isFixedEntryLen_(isFixedEntryLen) {
  if (cursor_->parent() != nullptr) {
    // non-leaf builder must work on MetaNode, which is of var len entry
    parent_builder_ = new NodeBuilder(cursor_->parent(), level + 1,
                                      MetaChunker::Instance(), false);
  }
  Resume();
}

void NodeBuilder::Resume() {
  int32_t original_idx = cursor_->idx();
  // place the cursor at start of SeqNode
  cursor_->seek(0);
  pre_cursor_seg_ = SegAtCursor();

  if (isFixedEntryLen_) {
    // if the entry is fixed length, no need to prolong each previous
    // entry one by one, we can prolong the segment from the head of the chunk
    // to the original_idx directly.
    const byte_t* addr = cursor_->current();
    cursor_->seek(original_idx);
    pre_cursor_seg_->prolong(cursor_->current() - addr);
  } else {
    while (cursor_->idx() != original_idx) {
      pre_cursor_seg_->prolong(cursor_->numCurrentBytes());
      cursor_->Advance(false);
    }
  }
}

void NodeBuilder::AppendSegmentEntries(const Segment* entry_seg) {
  commited_ = false;
  // DLOG(INFO) << "Append Seg # Entries: " << entry_seg->numEntries();
  appended_segs_.push_back(entry_seg);
}

size_t NodeBuilder::SkipEntries(size_t num_elements) {
  commited_ = false;
  // possible if this node builder is created during build process
  if (cursor_ == nullptr || cursor_->isEnd()) return 0;
  // DLOG(INFO) << "Level: " << level_ << " Before Skip Idx: " << cursor_->idx()
  //           << " Skip # Entry: " << num_elements;
  for (size_t i = 1; i <= num_elements; i++) {
    // whether the cursor points to the chunk end
    bool advance2end = !cursor_->Advance(false);

    if (advance2end)  {
      // Here, cursor has already reached this chunk end
      //   we need to skip and remove the parent metaentry
      //     which points to current chunk

     // Try to skip one entry in parent node builder
      bool end_parent = parent_builder_ == nullptr;

      if (!end_parent) {
        parent_builder()->SkipEntries(1);
        end_parent = parent_builder()->cursor_->isEnd();
      }

      if (end_parent) {
        // parent builder's cursor has also ADVANCED and reached to the seq end
        // therefore, current builder's cursor has also reached the seq end
        //   we can only skip i elements
        // Attempt to advance cursor to check
        // cursor indeed points the seq end and cannot advance
        //   even allowing crossing the boundary
        CHECK(cursor_->isEnd());
        num_skip_entries_ += i;
        return i;
      } else {
        // DLOG(INFO) << "Level: " << level_ << " Skipping Parent Recursive. "
        //           << "  # Entry this chunk: " <<  cursor_->idx();
        bool notEnd = cursor_->Advance(true);
        CHECK(notEnd);
      }  // end if parent_chunker
    }
  }    // end for
  num_skip_entries_ += num_elements;
  return num_elements;
}

void NodeBuilder::SpliceElements(size_t num_delete,
                                 const Segment* element_seg) {
  if (!commited_) {
    LOG(FATAL) << "There exists some uncommited operations. "
               << " Commit them first before doing any operation. ";
  }
  commited_ = false;

  size_t actual_delete = SkipEntries(num_delete);
  if (actual_delete < num_delete) {
    LOG(WARNING) << "Actual Remove " << actual_delete << " elements instead of "
                 << num_delete;
  }
  AppendSegmentEntries(element_seg);
}

NodeBuilder* NodeBuilder::parent_builder() {
  if (parent_builder_ == nullptr) {
    parent_builder_ =
        new NodeBuilder(level_ + 1, MetaChunker::Instance(), false);
  }
  return parent_builder_;
}

Chunk NodeBuilder::HandleBoundary(
    const std::vector<const Segment*>& segments) {
  // DLOG(INFO) << "Start Handing Boundary. ";
  ChunkInfo chunk_info = chunker_->Make(segments);

  Chunk& chunk = chunk_info.chunk;
  // Dump chunk into storage here
  store::GetChunkStore()->Put(chunk.hash(), chunk);

  parent_builder()->AppendSegmentEntries(chunk_info.meta_seg.get());
  created_segs_.push_back(std::move(chunk_info.meta_seg));
  rhasher_->ClearLastBoundary();
  return std::move(chunk);
}

Hash NodeBuilder::Commit() {
  CHECK(!commited_);
  // As we are about to make new chunk,
  // parent metaentry that points the old chunk
  // shall be invalid and replaced by the created metaentry
  // of the new chunk.
  // #ifdef DEBUG
  //   size_t skip_p = parent_builder()->SkipEntries(1);
  // if (skip_p > 0) {
  //   DLOG(INFO) << "Level: " << level_ << " Skipping Parents At Start. ";
  // }
  // #else
  parent_builder()->SkipEntries(1);
  // #endif  // define DEBUG
  // First thing to do:
  // Detect Boundary and Make Chunk
  //   Pass the MetaEntry to upper level builder
  Chunk last_created_chunk;
  // iterate newly appended entries for making chunk
  //   only detecing a boundary, make a chunk
  // DLOG(INFO) << "\n\nCommit Level: " << level_
  //           << " # Skip: " << num_skip_entries_
  //           << " # Appended Seg: " << appended_segs_.size();

  // vector of segments to make a chunk
  std::vector<const Segment*> chunk_segs;
  // The current seg to work on chunking
  const Segment* cur_seg = pre_cursor_seg_;
  size_t segIdx = 0;  // point to the next seg to handle after cur_seg

  if (cur_seg == nullptr) {
    // The cursur must be nullptr
    // if this builder is created by child recursively
    // Must have been appended some entries by child
    CHECK(!appended_segs_.empty());
    cur_seg = appended_segs_[0];
    segIdx = 1;
  }

  while (true) {
    CHECK(cur_seg != nullptr);
    bool hasBoundary = false;
    size_t num_entries = cur_seg->numEntries();
    for (size_t entryIdx = 0; entryIdx < num_entries; entryIdx++) {
      rhasher_->HashBytes(cur_seg->entry(entryIdx),
                          cur_seg->entryNumBytes(entryIdx));

      if (!rhasher_->CrossedBoundary()) continue;
      // Start to handle chunking
      hasBoundary = true;

      auto splitted_segs = cur_seg->Split(entryIdx + 1);
      chunk_segs.push_back(splitted_segs.first.get());
      last_created_chunk = HandleBoundary(chunk_segs);
      chunk_segs.clear();

      cur_seg = splitted_segs.second.get();
      created_segs_.push_back(std::move(splitted_segs.first));
      created_segs_.push_back(std::move(splitted_segs.second));

      break;
    }  // end of for entryIdx

    // Detect boundary at segment middle
    //   Start from loop head to handle the leftover segment after splitting
    if (hasBoundary && !cur_seg->empty()) continue;

    // No boundary at this segment
    //   append into chunk_segs
    if (!cur_seg->empty()) {
      chunk_segs.push_back(cur_seg);
    }

    if (segIdx < appended_segs_.size()) {
      cur_seg = appended_segs_[segIdx];
      ++segIdx;
    } else {
      break;
    }  // end of if
  }    // end of while

  // DLOG(INFO) << "# of Segments for Chunk: " << chunk_segs.size();

  // Second, combining original entries pointed by current cursor
  //   to make chunk
  Segment* work_seg = nullptr;
  if (cursor_ != nullptr && !cursor_->isEnd()) {
    work_seg = SegAtCursor();
    bool advanced = false;
    do {
      bool advanced2NextChunk = advanced && cursor_->idx() == 0;
      bool justHandleBoundary = rhasher_->byte_hashed() == 0;

      if (justHandleBoundary) {
        // if boundary is just handled in previous phase
        //   the first work_seg is init with current cursor, not nullptr.
        CHECK(!advanced || work_seg == nullptr);

        if (advanced2NextChunk) {
          break;
        }
        if (advanced) {
          work_seg = SegAtCursor();
        }
      } else {
        if (advanced2NextChunk) {
          parent_builder()->SkipEntries(1);
          // DLOG(INFO) << "Skip Parent During Concat."
          //           << " Cur Seg Bytes: " << work_seg->numBytes();
          CHECK(!work_seg->empty());
          chunk_segs.push_back(work_seg);

          work_seg = SegAtCursor();
        }  // end if advanced2NextChunk
      }    // end if justHandleBoundary

      size_t entry_num_bytes = cursor_->numCurrentBytes();
      work_seg->prolong(entry_num_bytes);
      rhasher_->HashBytes(cursor_->current(), entry_num_bytes);

      // Create Chunk and append metaentries to upper builders
      //   if detecing boundary
      if (rhasher_->CrossedBoundary()) {
        // Create chunk seg
        CHECK(!work_seg->empty());
        chunk_segs.push_back(work_seg);
        last_created_chunk = HandleBoundary(chunk_segs);
        chunk_segs.clear();

        work_seg = nullptr;  // To be Init next iteration
      }                      // end if
      advanced = true;
    } while (cursor_->Advance(true));
  }  // end if

  if (chunk_segs.size() > 0 || (work_seg != nullptr)) {
    // this could happen if the last entry of sequence
    // cannot form a boundary, we still need to make a explicit chunk
    // and append a metaentry to upper builder
    CHECK(cursor_ == nullptr ||
          (cursor_ != nullptr && cursor_->isEnd() && !cursor_->Advance(true)));

    if (work_seg != nullptr) {
      if (!work_seg->empty()) {
        chunk_segs.push_back(work_seg);
      }
    }

    last_created_chunk = HandleBoundary(chunk_segs);
    chunk_segs.clear();
  }  // end if
  // Comment the following line out
  //   because current NodeBuilder is allowed to commit for once.
  // commited_ = true;

  CHECK(!last_created_chunk.empty());
  // upper node builder would build a tree node with a single metaentry
  //   This node will be excluded from final prolley tree
  // DLOG(INFO) << "Finish one level commiting.\n";
  Hash root_key(last_created_chunk.hash().Clone());
  if (!parent_builder()->isInvalidNode()) {
    root_key = parent_builder()->Commit();
  }
  return root_key;
}

Segment* NodeBuilder::SegAtCursor() {
  Segment* seg;
  if (isFixedEntryLen_) {
    seg = new FixedSegment(cursor_->current(), cursor_->numCurrentBytes());
  } else {
    seg = new VarSegment(cursor_->current());
  }
  created_segs_.push_back(std::unique_ptr<const Segment>(seg));
  return seg;
}
}  // namespace ustore
