// Copyright (c) 2017 The Ustore Authors.

#include "node/node_builder.h"

#include <cstring>  // for memcpy
#include "hash/hash.h"
#include "node/node.h"
#include "node/orderedkey.h"

#include "utils/debug.h"
#include "utils/logging.h"

namespace ustore {
NodeBuilder::NodeBuilder(const Hash& root_hash, const OrderedKey& key,
                         ChunkLoader* chunk_loader, ChunkWriter* chunk_writer,
                         const Chunker* chunker, bool isFixedEntryLen) noexcept
    : NodeBuilder(std::unique_ptr<NodeCursor>(
                  new NodeCursor(root_hash, key, chunk_loader)),
                  0, chunk_writer, chunker, isFixedEntryLen) {}

// Perform operation at idx-th element at leaf rooted at root_hash
NodeBuilder::NodeBuilder(const Hash& root_hash, size_t idx,
                         ChunkLoader* chunk_loader, ChunkWriter* chunk_writer,
                         const Chunker* chunker, bool isFixedEntryLen) noexcept
    : NodeBuilder(std::unique_ptr<NodeCursor>(
                  new NodeCursor(root_hash, idx, chunk_loader)),
                  0, chunk_writer, chunker, isFixedEntryLen) {}

NodeBuilder::NodeBuilder(ChunkWriter* chunk_writer, const Chunker* chunker,
                         bool isFixedEntryLen) noexcept
    : NodeBuilder(0, chunk_writer, chunker, isFixedEntryLen) {}

NodeBuilder::NodeBuilder(std::unique_ptr<NodeCursor>&& cursor, size_t level,
                         ChunkWriter* chunk_writer, const Chunker* chunker,
                         bool isFixedEntryLen) noexcept
    : cursor_(std::move(cursor)),
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
      chunk_writer_(chunk_writer),
      chunker_(chunker),
      isFixedEntryLen_(isFixedEntryLen) {
  if (cursor_->parent() != nullptr) {
    // copy this node builder's parent cursor
    std::unique_ptr<NodeCursor> parent_cr(new NodeCursor(*cursor_->parent()));

    // non-leaf builder must work on MetaNode, which is of var len entry
    parent_builder_.reset(new NodeBuilder(std::move(parent_cr), level + 1,
                          chunk_writer_, MetaChunker::Instance(), false));
  }
  if (cursor_->node()->numEntries() > 0) {
    Resume();
  }
}

NodeBuilder::NodeBuilder(size_t level, ChunkWriter* chunk_writer,
                         const Chunker* chunker, bool isFixedEntryLen) noexcept
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
      chunk_writer_(chunk_writer),
      chunker_(chunker),
      isFixedEntryLen_(isFixedEntryLen) {
  // do nothing
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

    if (advance2end) {
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
  }  // end for
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
    parent_builder_.reset(new NodeBuilder(level_ + 1, chunk_writer_,
                          MetaChunker::Instance(), false));
  }
  return parent_builder_.get();
}

Chunk NodeBuilder::HandleBoundary(const std::vector<const Segment*>& segments) {
  // DLOG(INFO) << "Start Handing Boundary. ";
  ChunkInfo chunk_info = chunker_->Make(segments);

  Chunk& chunk = chunk_info.chunk;
  // Dump chunk into storage here
  chunk_writer_->Write(chunk.hash(), chunk);

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

    size_t boundary_pos =
        rhasher_->TryHashBytes(cur_seg->data(), cur_seg->numBytes());
    size_t entryIdx = cur_seg->PosToIdx(boundary_pos);
    if (rhasher_->CrossedBoundary()) {
      hasBoundary = true;
      auto splitted_segs = cur_seg->Split(entryIdx + 1);
      chunk_segs.push_back(splitted_segs.first.get());
      last_created_chunk = HandleBoundary(chunk_segs);
      chunk_segs.clear();
      cur_seg = splitted_segs.second.get();
      created_segs_.push_back(std::move(splitted_segs.first));
      created_segs_.push_back(std::move(splitted_segs.second));
    }

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
  if (cursor_ != nullptr && !cursor_->isEnd()) {
    CHECK_GE(cursor_->idx(), 0);
    // Get segment from node that spans from cursor pointed to node end
    const SeqNode* cursor_node = cursor_->node();
    size_t num_left_entries = cursor_node->numEntries()
                              - static_cast<size_t>(cursor_->idx());

    std::unique_ptr<const Segment> node_seg =
        cursor_node->GetSegment(cursor_->idx(), num_left_entries);
    DCHECK(!node_seg->empty());

    while (true) {
      size_t boundary_pos =
          rhasher_->TryHashBytes(node_seg->data(), node_seg->numBytes());

      if (boundary_pos == node_seg->numBytes()) {
        CHECK(!rhasher_->CrossedBoundary());
        // No boundary detected in this segment
        parent_builder()->SkipEntries(1);
        // DLOG(INFO) << "Skip Parent During Concat."
        //           << " Cur Seg Bytes: " << work_seg->numBytes();
        const Segment* node_seg_raw = node_seg.release();
        created_segs_.emplace_back(node_seg_raw);
        chunk_segs.push_back(node_seg_raw);

        // try to move cursor to the first element of next chunk
        num_left_entries = cursor_->node()->numEntries()
                            - static_cast<size_t>(cursor_->idx());
        // for (size_t i = 0; i < num_left_entries; ++i) {
        //   cursor_->Advance(true);
        // }
        cursor_->AdvanceEntry(num_left_entries);
        if (!cursor_->isEnd()) {
          DCHECK_EQ(0, cursor_->idx()) << cursor_->idx();
          // Prepare node_seq for next iteration
          cursor_node = cursor_->node();
          node_seg = cursor_node->GetSegment(cursor_->idx(),
                                             cursor_node->numEntries());
          DCHECK(!node_seg->empty());
          continue;
        } else {
          // cursor reaches the sequence end
          break;
        }  // end if cursor is end
      }  // end if (boundary_pos == node_seg->numBytes()) {

      // In the following case, a boundary is detected at segment middle
      DCHECK(rhasher_->CrossedBoundary());
      size_t entryIdx = node_seg->PosToIdx(boundary_pos);
      CHECK_LT(entryIdx, node_seg->numEntries());

      auto splitted_segs = node_seg->Split(entryIdx + 1);
      chunk_segs.push_back(splitted_segs.first.get());
      last_created_chunk = HandleBoundary(chunk_segs);
      chunk_segs.clear();

      size_t numEntry = node_seg->numEntries();
      created_segs_.push_back(std::move(node_seg));
      created_segs_.push_back(std::move(splitted_segs.first));

      node_seg = std::move(splitted_segs.second);
      // A boundary is detected at the last element in the segment
      if (entryIdx == numEntry - 1) {
        DCHECK(node_seg->empty());
        break;
      } else {
        DCHECK(!node_seg->empty());
      }
    }  // end while
  }  // end if

  if (chunk_segs.size() > 0) {
    // this could happen if the last entry of sequence
    // cannot form a boundary, we still need to make a explicit chunk
    // and append a metaentry to upper builder
    CHECK(cursor_ == nullptr ||
          (cursor_ != nullptr && cursor_->isEnd() && !cursor_->Advance(true)));

    last_created_chunk = HandleBoundary(chunk_segs);
    chunk_segs.clear();
  }  // end if
  // Comment the following line out
  //   because current NodeBuilder is allowed to commit for once.
  // commited_ = true;
  // CHECK(!last_created_chunk.empty());
  if (last_created_chunk.empty()) {
    // This occurs when the resulting prolly tree does not contain
    //   any elements, create an empty chunk
    last_created_chunk = HandleBoundary({});
  }
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
  created_segs_.emplace_back(seg);
  return seg;
}

/////////////////////////////////////////////////////////
// Advanced Node Builder
AdvancedNodeBuilder::AdvancedNodeBuilder(const Hash& root,
                                         ChunkLoader* loader,
                                         ChunkWriter* writer) :
    AdvancedNodeBuilder(0, root, loader, writer) {}

AdvancedNodeBuilder::AdvancedNodeBuilder(size_t level,
                                         const Hash& root,
                                         ChunkLoader* loader,
                                         ChunkWriter* writer) :
    level_(level),
    root_(root), loader_(loader), writer_(writer),
#ifdef TEST_NODEBUILDER
    rhasher_(RollingHasher::TestHasher()),
#else
    rhasher_(new RollingHasher()),
#endif
    parent_builder_(),  // init with empty
    operands_(),  // init with empty list
    created_segs_() {}  // init with empty created segs

AdvancedNodeBuilder::AdvancedNodeBuilder(ChunkWriter* writer) :
    AdvancedNodeBuilder(0, writer) {}

AdvancedNodeBuilder::AdvancedNodeBuilder(size_t level, ChunkWriter* writer) :
    AdvancedNodeBuilder(level, Hash(), nullptr, writer) {}

AdvancedNodeBuilder& AdvancedNodeBuilder::Splice(
    uint64_t start_idx, uint64_t num_delete,
    const std::vector<const Segment*>& segs) {

  std::unique_ptr<NodeCursor> cr;
  uint64_t total_num_elements = 0;

  do {
    if (root_.empty()) {
      if (start_idx > 0 || num_delete > 0) {
        LOG(WARNING) << " Can not splice on an empty tree "
                     << " from index " << start_idx
                     << " to remove " << num_delete << " elements."
                     << "\n"
                     << " Operation Failed. ";
        break;
      }
    } else {
      NodeCursor* tmp_cr = new NodeCursor(root_, start_idx, loader_);

      if (tmp_cr->empty()) {
        LOG(WARNING) << "Start_idx exceeds the total number of elements."
                     << "Or, the tree with root hash "
                     << root_.ToBase32() << " does not exist. "
                     << "\n"
                     << " Operation Failed. ";
        break;
      }

      cr.reset(tmp_cr);
      total_num_elements =
          SeqNode::CreateFromChunk(loader_->Load(root_))->numElements();
    }

    std::vector<const Segment*> operand_segs;

    for (const Segment* seg : segs) {
      if (!seg->empty()) operand_segs.push_back(seg);
    }

    SpliceOperand operand{start_idx,
                          std::move(cr),
                          num_delete,
                          std::move(operand_segs)};

    bool inserted = false;
    uint64_t pre_start_idx = 0;
    uint64_t pre_num_delete = 0;

    // Insert the created operand into operand list based on start_idx order
    auto it = operands_.begin();
    for (; it != operands_.end(); ++it) {
      if (it->start_idx < start_idx) {
        // do nothing
      } else if (it->start_idx == start_idx) {
        LOG(WARNING) << "Previous Operation has already worked "
                     << " on element with index "
                     << start_idx << ".\n"
                     << "Operation Failed.";
        } else {
      // for the case (it->start_idx > start_idx) {
        if (pre_start_idx + pre_num_delete > start_idx) {
          LOG(WARNING) << "Start_idx elements may already "
                       << " be removed by previous splice operation. \n"
                       << "Operation Failed.";

          // actually NOT inserted
          // make inserted true to exit the do-while loop
          inserted = true;
          break;
        }
        operands_.insert(it, std::move(operand));
        inserted = true;
      }
      pre_start_idx = it->start_idx;
      pre_num_delete = it->num_delete;
    }

    // insert at the end
    if (!inserted) {
      CHECK(it == operands_.end());
      if (start_idx + num_delete > total_num_elements) {
         LOG(WARNING) << "The index of elements to remove exceeds "
                      << " the total number of elements. \n";
      }
      operands_.insert(it, std::move(operand));
    }
  } while (0);
  return *this;
}

// Return the root hash of the updated tree
//   the hash owns its internal data
Hash AdvancedNodeBuilder::Commit(const Chunker& chunker,
                                 bool isFixedEntryLen) {
  DLOG(INFO) << "\n\nStart to Commit Level " << level_;
  if (operands_.size() == 0) {
    if (root_.empty()) {
      // Create a single node with empty contents
      ChunkInfo chunk_info = HandleBoundary(chunker, {});
      return chunk_info.chunk.hash().Clone();
    } else {
      LOG(WARNING) << "No Operation to Commit.";
      return root_;
    }
  }

#ifdef DEBUG
  uint64_t pre_start_idx = 0;
  // size_t i = 0;
  for (const auto& operand : operands_) {
    // size_t num_entries = 0;
    // for (const Segment* seg : operand.appended_segs) {
    //   num_entries += seg->numEntries();
    // }

    // DLOG(INFO) << "Operand " << i << " "
    //            << "  Element Idx: " << operand.start_idx
    //            << "  Cursor Idx: " << operand.cursor->idx()
    //            << "  # of Entries in Cursor :"
    //            << operand.cursor->node()->numEntries()
    //            << " # to remove: " << operand.num_delete
    //            << " # of appended segs: "
    //            << operand.appended_segs.size()
    //            << " with # of entries: " << num_entries;

    // ++i;

    // CHECK all segments are non-empty
    for (const Segment* seg : operand.appended_segs) {
      DCHECK(!seg->empty());
    }

    if (!operand.cursor && level_ == 0) {
      // operation works on a fresh new tree
      DCHECK(root_.empty());
      DCHECK_EQ(size_t(0), operand.start_idx);
      DCHECK_EQ(size_t(0), operand.num_delete);
      // must have segments to append
      DCHECK_LT(size_t(0), operand.appended_segs.size());
      DCHECK_EQ(size_t(1), operands_.size());
    }
    // start_idx of operands are in strict ascending order
    DCHECK(operand.start_idx == 0 || pre_start_idx < operand.start_idx);

    pre_start_idx = operand.start_idx;
  }
#endif

  /* for each operand, the tree update consists of the following three phases:
  Phase 1: * Create the segment from chunk start until cursor (exclusive)
           * Only Execute this phase if new_round = true
  Phase 2: * Phase 2.1: Remove elements by skipping cursor
             * During the traversing, if reaching the element pointed by the cursor
               in the next operand, go to phase 2 directly for next operand. new_round is set to false
           * Phase 2.2: Traverse and append the appended segments
             * Detect Boundary pattern and create operands for parent builder
  Phase 3: * Traverse the original subsequent elements until a boundary is detected
             or the sequence reaches the end
           * During the traversing, if reaching the element pointed by the cursor
             in the next operand, go to phase 2 directly for next operand. new_round is set to false
  */

  bool new_round = true;
  rhasher_->ClearLastBoundary();

  // a vector of segments to make chunk
  std::vector<const Segment*> chunk_segs;

  // curent segment to work on for phase 2
  const Segment* p2_work_seg = nullptr;
  Chunk last_created_chunk;

// operands to be constructed for parent node builder
  uint64_t parent_operand_start_idx = 0;
  // number of entries to remove by parent builder
  //   equal to the number of boundary crossed in current builder
  uint64_t parent_num_delete = 0;
  std::unique_ptr<NodeCursor> parent_cr;
  std::vector<const Segment*> parent_appended_segs;

  for (auto operand_it = operands_.begin();
       operand_it != operands_.end();
       ++operand_it) {
    DLOG(INFO) << "\n===========NEW Iteration=============\n"
               << "New Operand at starting index: "
               << operand_it->start_idx
               << " to remove " << operand_it->num_delete
               << " elements and append " << operand_it->appended_segs.size()
               << " segments.";
    bool next_cr_encountered = false;

    // construct iterator to next operand
    ++operand_it;
    std::list<SpliceOperand>::iterator next_operand_it(operand_it);
    --operand_it;

#ifdef DEBUG
    if (next_operand_it == operands_.end()) {
      DLOG(INFO) << "Next Operand: End ";
    }
#endif

    // Point to the next seg in operand appended segs
    //   to handle after p2_work_seg
    size_t segIdx = 0;

    // the operand has nonempty cursor
    if (operand_it->cursor &&
        operand_it->cursor->node()->numEntries()) {
      if (new_round) {
        // Phase 1
        DLOG(INFO) << "\nPhase 1: \tStarting New Round.";

        // Initialize the operand for parent builder
        parent_operand_start_idx = operand_it->start_idx;
        parent_num_delete = 0;
        parent_appended_segs.clear();

        auto parent_cursor = operand_it->cursor->parent();
        if (parent_cursor) {
          parent_cr.reset(new NodeCursor(*parent_cursor));
        }

        DCHECK(chunk_segs.empty());
        chunk_segs.clear();

        p2_work_seg = InitPreCursorSeg(isFixedEntryLen, *operand_it->cursor);
      } else {
        // If NOT new round,
        // p2_work_seg = p3_work_seg in previous operand iteration
        // No need to create precursor seg
        if (!p2_work_seg) {
          if (operand_it->appended_segs.size() > 0) {
            p2_work_seg = operand_it->appended_segs[0];
            segIdx = 1;
          }
        }  // if !p2_work_sg
      }  // end if (operand_it->cursor)

      // Phase 2.1: Remove elements by traversing cursor
      //
      //   if encountering the cursor in the next operand
      //     put current operand appended segs into
      //     the head of appended segs in the next operand
      DLOG(INFO) << "\nPhase 2.1 Skipping Entries At Cursor Entry Index "
                 << operand_it->cursor->idx()
                 << " Total Entries: "
                 << operand_it->cursor->node()->numEntries();

      for (uint64_t i = 1; i <= operand_it->num_delete; ++i) {
        operand_it->cursor->Advance(false);
        if (operand_it->cursor->isEnd()) {
          // cursor is at chunk end.
          // When crossing a chunk boundary,
          //   one more parent entry needs to remove
          ++parent_num_delete;
          DLOG(INFO) << "Incrementing parent_to_delete to: "
                     << parent_num_delete;
          bool isSeqEnd = !operand_it->cursor->Advance(true);

          if (isSeqEnd && i < operand_it->num_delete) {
            DLOG(INFO) << "Only Remove " << i << " entries to reach seq end."
                       << " out of " << operand_it->num_delete
                       << " required.";
            break;
          }
        }  // end if chunkEnd

        next_cr_encountered =
          next_operand_it != operands_.end() &&
          next_operand_it->cursor &&
          *next_operand_it->cursor == *operand_it->cursor;

        if (next_cr_encountered) {
          DLOG(INFO) << "\tNext Cursor Encountered " << i
                     << " elements after current cursor";

          next_operand_it->appended_segs.insert(
            next_operand_it->appended_segs.begin(),
            operand_it->appended_segs.begin(),
            operand_it->appended_segs.end());

          if (segIdx == 1) p2_work_seg = nullptr;
          new_round = false;
          break;  // will restart for a new iteration for the next operand
        }
      }  // end for num_delete
    } else {
      // if no cursor at level 0,
      //   the prolley tree is built from scratch
      // must have a single segment to append
      DCHECK(level_ != 0 || operands_.size() == 1);
      DCHECK_LT(size_t(0), operands_.size());
      p2_work_seg = operand_it->appended_segs[0];
      segIdx = 1;
    }  // if operand_it->cursor

    // start from for loop start
    if (next_cr_encountered) {continue; }

    // Phase 2.2: Append for appended segs in the operand
    DLOG(INFO) << "\nPhase 2.2 Appending Segments.";
    while (p2_work_seg) {
      bool hasBoundary = false;
      DLOG(INFO) << "\t# of entries in p2_work_seg: "
                 << p2_work_seg->numEntries();
      // DLOG(INFO) << "\t# of entries in p2_work_seg: " << num_entries;

      size_t boundary_pos =
          rhasher_->TryHashBytes(p2_work_seg->data(),
                                 p2_work_seg->numBytes());
      size_t entryIdx = p2_work_seg->PosToIdx(boundary_pos);
      DLOG(INFO) << "EntryIdx: " << entryIdx;

      if (rhasher_->CrossedBoundary()) {
        // Start to handle chunking
        hasBoundary = true;

        auto splitted_segs = p2_work_seg->Split(entryIdx + 1);
        DCHECK(!splitted_segs.first->empty());

        chunk_segs.push_back(splitted_segs.first.get());

        ChunkInfo chunk_info = HandleBoundary(chunker, chunk_segs);
        last_created_chunk = std::move(chunk_info.chunk);

        parent_appended_segs.push_back(chunk_info.meta_seg.get());
        created_segs_.push_back(std::move(chunk_info.meta_seg));

        chunk_segs.clear();

        p2_work_seg = splitted_segs.second.get();
        created_segs_.push_back(std::move(splitted_segs.first));
        created_segs_.push_back(std::move(splitted_segs.second));
      }

      // Detect boundary at segment middle
      //   Start from loop head to handle the leftover segment after splitting
      if (hasBoundary && !p2_work_seg->empty()) continue;

      // No boundary at this segment
      //   append into chunk_segs
      if (!p2_work_seg->empty()) {
        chunk_segs.push_back(p2_work_seg);
      }

      if (segIdx < operand_it->appended_segs.size()) {
        p2_work_seg = operand_it->appended_segs[segIdx];
        ++segIdx;
      } else {
        p2_work_seg = nullptr;
        // end the loop
      }  // end of if
    }  // end of while (true)

    // Phase 3: Concat subsequent elements,
    //   Detect Boundary and Create chunk accordingly
    DLOG(INFO) << "\nPhase 3: Concat Subsequent Entries";
    Segment* p3_work_seg = nullptr;

    if (operand_it->cursor && !operand_it->cursor->isEnd()) {
      DLOG(INFO) << "\tCurrent operand cursor entry idx: "
                 << operand_it->cursor->idx();
      p3_work_seg = InitCursorSeg(isFixedEntryLen, *operand_it->cursor);
      bool advanced = false;
      bool isSeqEnd = false;
      do {
        bool advanced2NextChunk = advanced && operand_it->cursor->idx() == 0;
        bool justHandleBoundary = rhasher_->byte_hashed() == 0;

        if (justHandleBoundary) {
          // if boundary is just handled in previous phase
          //   the first p3_work_seg is init with current cursor, not nullptr.
          CHECK(!advanced || p3_work_seg == nullptr);
          DLOG(INFO) << "Just Handle Boundary. ";
          if (advanced2NextChunk) {
            DLOG(INFO) << "Advance to Next Chunk after boundary handling";
            break;  // break the do-while loop to end phase 3
          }

          DLOG(INFO) << "Init Empty Cursor Segment for p3_work_seg";
          p3_work_seg = InitCursorSeg(isFixedEntryLen, *operand_it->cursor);
        } else {
          if (advanced2NextChunk) {
            DLOG(INFO) << "Advance to Next Chunk without detecting boundary. ";
            ++parent_num_delete;
            DLOG(INFO) << "Incrementing parent_to_delete to: "
                       << parent_num_delete;
            CHECK(!p3_work_seg->empty());
            chunk_segs.push_back(p3_work_seg);

            p3_work_seg = InitCursorSeg(isFixedEntryLen, *operand_it->cursor);
          }  // end if advanced2NextChunk
        }  // end if justHandleBoundary

      // Detecting identical cursor
        next_cr_encountered =
          next_operand_it != operands_.end() &&
          next_operand_it->cursor &&
          *next_operand_it->cursor == *operand_it->cursor;

        // if encountering the next cursor
        //   Copy the p2_work_seg from p3_work_seg
        //   to for phase 2 in next iteration
        if (next_cr_encountered) {
          DLOG(INFO) << "\tNext Cursor Encountered at entry idx"
                     << operand_it->cursor->idx();
          if (!p3_work_seg->empty()) {
            chunk_segs.push_back(p3_work_seg);
          }
          p2_work_seg = nullptr;
          new_round = false;
          break;
        }

        size_t entry_num_bytes = operand_it->cursor->numCurrentBytes();
        DLOG(INFO) << "Prolong " << entry_num_bytes << " Bytes. ";
        p3_work_seg->prolong(entry_num_bytes);
        rhasher_->HashBytes(operand_it->cursor->current(), entry_num_bytes);

        // Create Chunk and append metaentries to upper builders
        //   if detecing boundary
        if (rhasher_->CrossedBoundary()) {
          DLOG(INFO) << "Crossing Boundary after prolong ";
          // Create chunk seg
          CHECK(!p3_work_seg->empty());
          chunk_segs.push_back(p3_work_seg);

          ChunkInfo chunk_info = HandleBoundary(chunker, chunk_segs);
          last_created_chunk = std::move(chunk_info.chunk);

          parent_appended_segs.push_back(chunk_info.meta_seg.get());
          created_segs_.push_back(std::move(chunk_info.meta_seg));

          chunk_segs.clear();

          p3_work_seg = nullptr;  // To be Init next iteration of the do loop
        }  // end if rhasher crossed boundary

        advanced = true;
        isSeqEnd = !operand_it->cursor->Advance(true);
        // DLOG(INFO) << "||Current cursor entry idx: "
        //            << operand_it->cursor->idx() << "||";

        // iteration continues until both cursor and operand_it
        //   have reached the end
      } while (!(isSeqEnd && next_operand_it == operands_.end()));

      if (next_cr_encountered) {continue; }  // start from for loop
    }  // end if (operand_it->cursor && !operand_it->cursor->isEnd())

    if (operand_it->cursor) {
      ++parent_num_delete;
      DLOG(INFO) << "Incrementing parent_to_delete to: "
                 << parent_num_delete;
    }

    if (p3_work_seg != nullptr && !p3_work_seg->empty()) {
      chunk_segs.push_back(p3_work_seg);
    }

    if (last_created_chunk.empty() || chunk_segs.size() > 0) {
      DLOG(INFO) << "Detecting Implicit Boundary at seq end.";
      // this could happen if the last entry of sequence
      // cannot form a boundary, we still need to make a implicit chunk
      // and append a metaentry to upper builder
      DCHECK(!operand_it->cursor ||
            (operand_it->cursor->isEnd() &&
             !operand_it->cursor->Advance(true)));

      // this operand must be the last one
      DCHECK(next_operand_it == operands_.end());

      ChunkInfo chunk_info = HandleBoundary(chunker, chunk_segs);
      last_created_chunk = std::move(chunk_info.chunk);
      parent_appended_segs.push_back(chunk_info.meta_seg.get());
      created_segs_.push_back(std::move(chunk_info.meta_seg));
    }

    DLOG(INFO) << "\n Final Phase: ";
    DLOG(INFO) << "\tPush operand for parent: "
               << " start_idx: " << parent_operand_start_idx
               << " num_delete: " << parent_num_delete
               << " # appended segs: " << parent_appended_segs.size();

    parent()->operands_.push_back({parent_operand_start_idx,
                                  std::move(parent_cr),
                                  parent_num_delete,
                                  parent_appended_segs});
    new_round = true;
    rhasher_->ClearLastBoundary();
  }   // end for operands


  operands_.clear();
  CHECK(!last_created_chunk.empty());

  // upper node builder would build a tree node with a single metaentry
  //   This node will be excluded from final prolley tree
  // DLOG(INFO) << "Finish one level commiting.\n";
  Hash root_key(last_created_chunk.hash().Clone());

  bool valid_parent_builder = false;

  // invalid parent builder must satisfy ALL the following conditions :
  // * It contain exactly one operand
  // * This operand contains only one segment
  // * This operand cursor is nullptr
  do {
    DLOG(INFO) << "# of parent operands: " <<
               parent()->operands_.size();
    if (parent()->operands_.size() > 1) {
      valid_parent_builder = true;
      break;
    }

    DLOG(INFO) << "# of parent operand segments: " <<
               parent()->operands_.begin()->appended_segs.size();

    if (parent_builder_->operands_.begin()->appended_segs.size() > 1) {
      valid_parent_builder = true;
      break;
    }


    NodeCursor* parent_operand_cursor =
        parent()->operands_.begin()->cursor.get();

    if (parent_operand_cursor) {
      valid_parent_builder = true;
      break;
    }
  } while (0);

  if (valid_parent_builder) {
    Hash parent_hash = parent()->Commit(*MetaChunker::Instance(), false);
    if (parent()->num_created_entries_ > 1) {
      return parent_hash;
    } else {
      return root_key;
    }
  }
  return root_key;
}

AdvancedNodeBuilder* AdvancedNodeBuilder::parent() {
  if (!parent_builder_) {
    parent_builder_.reset(
      new AdvancedNodeBuilder(level_ + 1, root_, loader_, writer_));
  }
  return parent_builder_.get();
}

Segment* AdvancedNodeBuilder::InitCursorSeg(bool isFixedEntryLen,
                                            const NodeCursor& cursor) {
  Segment* seg = nullptr;
  if (isFixedEntryLen) {
    seg = new FixedSegment(cursor.current(),
                           cursor.numCurrentBytes());
  } else {
    seg = new VarSegment(cursor.current());
  }
  created_segs_.push_back(std::unique_ptr<const Segment>(seg));
  return seg;
}

const Segment* AdvancedNodeBuilder::InitPreCursorSeg(
    bool isFixedEntryLen, const NodeCursor& cursor) {
  DLOG(INFO) << "Init Precursor";
  NodeCursor cr(cursor);
  int32_t original_idx = cr.idx();

  cr.seek(0);  // place the cr at start of SeqNode
  Segment* pre_cr_seg = InitCursorSeg(isFixedEntryLen, cr);

  if (isFixedEntryLen) {
    // if the entry is fixed length, no need to prolong each previous
    // entry one by one, we can prolong the segment from the head of the chunk
    // to the original_idx directly.
    const byte_t* addr = cr.current();
    cr.seek(original_idx);
    pre_cr_seg->prolong(cr.current() - addr);
  } else {
    while (cr.idx() != original_idx) {
      pre_cr_seg->prolong(cr.numCurrentBytes());
      cr.Advance(false);
    }
  }

  return pre_cr_seg;
}


ChunkInfo AdvancedNodeBuilder::HandleBoundary(
    const Chunker& chunker, const std::vector<const Segment*>& segments) {
  // DLOG(INFO) << "Start Handing Boundary. ";
  ChunkInfo chunk_info = chunker.Make(segments);
  rhasher_->ClearLastBoundary();

  for (const Segment* seg : segments) {
    this->num_created_entries_ += seg->numEntries();
  }
  // Dump chunk into storage here
  writer_->Write(chunk_info.chunk.hash(), chunk_info.chunk);

#ifdef DEBUG
  size_t total_entries = 0;
  for (const auto& seg : segments) {
    total_entries += seg->numEntries();
  }

  DLOG(INFO) << "Handle Boundary For "
             << segments.size() << " segments with total "
             << total_entries << " entries. ";

  // for (size_t s = 0; s < segments.size(); s++) {
  //   DLOG(INFO) << "\tSeg " << s << ": "
  //              << segments[s]->numEntries();
  // }
#endif
  return chunk_info;
}

Chunk AdvancedNodeBuilder::ChunkCacher::GetChunk(const Hash& key) {
  auto cache_it = cache_.find(key);
  auto has_read_it = has_read_.find(key);

  if (cache_it != cache_.end()) {
    CHECK(has_read_it != has_read_.end());
    has_read_it->second = true;
    return Chunk(cache_it->second);
  }
  return loader_->GetChunk(key);
}

bool AdvancedNodeBuilder::ChunkCacher::DumpUnreadCacheChunk() {
  bool all = true;
  for (const auto& kv : cache_) {
    auto has_read_it = has_read_.find(kv.first);
    CHECK(has_read_it != has_read_.end());

    if (!has_read_it->second) {
      bool result = writer_->Write(kv.first, Chunk(kv.second));
      all = all && result;
    }
  }
  return all;
}

}  // namespace ustore
