// Copyright (c) 2017 The Ustore Authors.

#include "node/node_builder.h"

#include <algorithm>  // for memcpy
#include <cstring>  // for memcpy
#include "hash/hash.h"
#include "node/meta_node.h"
#include "node/orderedkey.h"

#include "utils/debug.h"
#include "utils/logging.h"

namespace ustore {
NodeBuilder::NodeBuilder(const Hash& root_hash, const OrderedKey& key,
  ChunkLoader* chunk_loader, ChunkWriter* chunk_writer, const Chunker* chunker,
  const Chunker* parent_chunker) noexcept
    : NodeBuilder(std::unique_ptr<NodeCursor>(
                  new NodeCursor(root_hash, key, chunk_loader)),
                  0, chunk_writer, chunker, parent_chunker) {}

// Perform operation at idx-th element at leaf rooted at root_hash
NodeBuilder::NodeBuilder(const Hash& root_hash, size_t idx,
  ChunkLoader* chunk_loader, ChunkWriter* chunk_writer, const Chunker* chunker,
  const Chunker* parent_chunker) noexcept
    : NodeBuilder(std::unique_ptr<NodeCursor>(
                  new NodeCursor(root_hash, idx, chunk_loader)),
                  0, chunk_writer, chunker, parent_chunker) {}

NodeBuilder::NodeBuilder(ChunkWriter* chunk_writer, const Chunker* chunker,
    const Chunker* parent_chunker) noexcept
    : NodeBuilder(0, chunk_writer, chunker, parent_chunker) {}

NodeBuilder::NodeBuilder(std::unique_ptr<NodeCursor>&& cursor, size_t level,
                         ChunkWriter* chunk_writer, const Chunker* chunker,
                         const Chunker* parent_chunker) noexcept
    : cursor_(std::move(cursor)),
      parent_builder_(nullptr),
      appended_segs_(),
      created_segs_(),
      pre_cursor_seg_(nullptr),
      rhasher_(chunker->GetRHasher()),
      commited_(true),
      num_skip_entries_(0),
      level_(level),
      chunk_writer_(chunk_writer),
      chunker_(chunker),
      parent_chunker_(parent_chunker) {
  if (cursor_->parent() != nullptr) {
    // copy this node builder's parent cursor
    std::unique_ptr<NodeCursor> parent_cr(new NodeCursor(*cursor_->parent()));

    // non-leaf builder must work on MetaNode, which is of var len entry
    parent_builder_.reset(new NodeBuilder(std::move(parent_cr), level + 1,
        chunk_writer_, parent_chunker, parent_chunker));
  }
  if (cursor_->node()->numEntries() > 0) {
    Resume();
  }
}

NodeBuilder::NodeBuilder(size_t level, ChunkWriter* chunk_writer,
  const Chunker* chunker, const Chunker* parent_chunker) noexcept
    : cursor_(nullptr),
      parent_builder_(nullptr),
      appended_segs_(),
      created_segs_(),
      pre_cursor_seg_(nullptr),
      rhasher_(chunker->GetRHasher()),
      commited_(true),
      num_skip_entries_(0),
      level_(level),
      chunk_writer_(chunk_writer),
      chunker_(chunker),
      parent_chunker_(parent_chunker) {
  // do nothing
}

void NodeBuilder::Resume() {
  int32_t original_idx = cursor_->idx();
  // place the cursor at start of SeqNode
  cursor_->seek(0);
  pre_cursor_seg_ = SegAtCursor();

  if (chunker_->isFixedEntryLen()) {
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

void NodeBuilder::SpliceElements(size_t num_delete,
                                 std::vector<const Segment*> element_segs) {
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

  appended_segs_.insert(appended_segs_.end(), element_segs.begin(),
                        element_segs.end());
}

NodeBuilder* NodeBuilder::parent_builder() {
  if (parent_builder_ == nullptr) {
    parent_builder_.reset(new NodeBuilder(level_ + 1, chunk_writer_,
                          parent_chunker_, parent_chunker_));
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
  Hash root = RecursiveCommit();
  if (cursor_) {
    //  The tree is NOT constructed from scratch but updated on an existing tree
    //  It is possible that its height shall be reduced for removing elements.
    //  We need to prune away the root node from top
    //    if the root node contains a single metaentry.
    const Chunk* root_chunk = cursor_->loader()->Load(root);
    auto root_node = SeqNode::CreateFromChunk(root_chunk);

    while (!root_node->isLeaf() && root_node->numEntries() == 1) {
      const MetaNode* mnode = dynamic_cast<const MetaNode*>(root_node.get());
      root = mnode->GetChildHashByEntry(0);
      root_chunk = cursor_->loader()->Load(root);
      root_node = SeqNode::CreateFromChunk(root_chunk);
    }  // end while
  }  // end if cursor_

  return root.Clone();
}

Hash NodeBuilder::RecursiveCommit() {
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

  if (cur_seg == nullptr && !appended_segs_.empty()) {
    cur_seg = appended_segs_[0];
    segIdx = 1;
  }

  while (cur_seg) {
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
    // This occurs when the resulting pos tree does not contain
    //   any elements, create an empty chunk
    last_created_chunk = HandleBoundary({});
  }
  // upper node builder would build a tree node with a single metaentry
  //   This node will be excluded from final pos tree
  // DLOG(INFO) << "Finish one level commiting.\n";
  Hash root_hash(last_created_chunk.hash().Clone());
  if (parent_builder()->cursor_ != nullptr ||
      parent_builder()->numAppendSegs() > 1) {
    root_hash = parent_builder()->RecursiveCommit();
  }  // end if parent_builder
  return root_hash;
}

Segment* NodeBuilder::SegAtCursor() {
  Segment* seg;
  if (chunker_->isFixedEntryLen()) {
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
    root_(root), loader_(loader), writer_(writer),
    operands_(), all_operand_segs_() {}


AdvancedNodeBuilder::AdvancedNodeBuilder(ChunkWriter* writer) :
    AdvancedNodeBuilder(Hash(), nullptr, writer) {}

AdvancedNodeBuilder& AdvancedNodeBuilder::Splice(
    uint64_t start_idx, uint64_t num_delete,
    std::vector<std::unique_ptr<const Segment>> segs) {

  uint64_t total_num_elements = 0;
  if (!root_.empty()) {
    total_num_elements =
        SeqNode::CreateFromChunk(loader_->Load(root_))->numElements();
  }

  if (total_num_elements < start_idx) {
    LOG(FATAL) << "The index of the first working element exceeds "
               << " the total number of elements.";
  } else if (total_num_elements < start_idx + num_delete) {
    LOG(WARNING) << "The index of elements to remove exceeds "
      << " the total number of elements. "
      << " Number of removal will be shortened to (total_num - start_idx).";
    num_delete = total_num_elements - start_idx;
  }

  // will be passed as SpliceOperand member
  std::vector<const Segment*> operand_segs;
  for (auto seg_it = segs.begin(); seg_it != segs.end(); ++seg_it) {
    if ((*seg_it)->empty()) continue;
    operand_segs.push_back((*seg_it).get());
    // Pass operand segments to all_operands_seg_ to manage the lifetime
    all_operand_segs_.push_back(std::move(*seg_it));
  }  // end for

  /* Each SpliceOperand defines an closed-open working interval,
   * e.g, [1, 3) [3, 5),
   * we need to find a slot between two sorted intervals in operands
   * to insert this new operand.
   * And need to ensure that no intervals overlap.
   */
  auto operand_after_tail_it =
    std::lower_bound(operands_.begin(), operands_.end(), start_idx + num_delete,
                     [](const SpliceOperand& operand, uint64_t pos) -> bool {
                     return operand.start_idx < pos; });
#ifdef DEBUG
  if (operand_after_tail_it != operands_.end()) {
    DCHECK_LE(start_idx + num_delete, operand_after_tail_it->start_idx);
  }
#endif

  auto operand_after_head_it =
    std::upper_bound(operands_.begin(), operands_.end(), start_idx,
                     [](uint64_t pos, const SpliceOperand& operand) -> bool {
                     return pos < operand.start_idx + operand.num_delete; });
#ifdef DEBUG
  if (operand_after_head_it != operands_.end()) {
    DCHECK_LT(start_idx,
      operand_after_head_it->start_idx + operand_after_head_it->num_delete);
  }
#endif

  if (operand_after_head_it == operand_after_tail_it) {
    operands_.insert(operand_after_tail_it,
                     {start_idx, num_delete, std::move(operand_segs)});
  } else if (--operand_after_head_it == operand_after_tail_it) {
    /* Deal with an exceptional condition that this new operand INSERTs at a
       position that is INSERTED by another operand.
       Two inserted segments will be combined for insertion in order.
    */
    CHECK_EQ(size_t(0), num_delete);
    CHECK_EQ(size_t(0), operand_after_tail_it->num_delete);
    operand_after_tail_it->appended_segs.insert(
      operand_after_tail_it->appended_segs.end(),
      operand_segs.begin(), operand_segs.end());
  } else {
    LOG(FATAL) << "Start_idx elements may already "
                 << " be removed by previous splice operation. \n"
                 << "Operation Failed.";
  }

#ifdef DEBUG
  // Check SpliceOperands are organized in consistent order.
  uint64_t pre_end = 0;
  for (const auto& operand : operands_) {
    DCHECK_LE(pre_end, operand.start_idx);
    pre_end = operand.start_idx + operand.num_delete;
  }
  DCHECK_LE(pre_end, total_num_elements);
#endif
  return *this;
}

// Return the root hash of the updated tree
//   the hash owns its internal data
Hash AdvancedNodeBuilder::Commit(const Chunker& chunker) {
  if (root_.empty()) {
    if (operands_.size() == 0) {
      // Create an empty object

      NodeBuilder nb(writer_, &chunker,
                     MetaChunker::Instance());
      nb.SpliceElements(0, {});
      return nb.Commit();
    } else if (operands_.size() == 1) {
      // Init the object with a single operand
      NodeBuilder nb(writer_, &chunker,
                     MetaChunker::Instance());
      nb.SpliceElements(operands_.begin()->num_delete,
                        operands_.begin()->appended_segs);
      return nb.Commit();
    } else {
      LOG(WARNING) << "No Multiple Operations on empty tree.";
      return root_;
    }  // end if operands.size() == 0
  }  // end if root_.empty()

  PersistentChunker persistentMetaChunker(MetaChunker::Instance());
  PersistentChunker persistentLeafChunker(&chunker);

  ChunkCacher chunk_cacher(loader_, writer_);

  Hash base = root_;
  for (auto operand_it = operands_.rbegin();
       operand_it != operands_.rend();
       operand_it++) {
    // Traverse the list in reverse order
    NodeBuilder nb(base, operand_it->start_idx,
                   &chunk_cacher, &chunk_cacher,
                   &persistentLeafChunker, &persistentMetaChunker);

    nb.SpliceElements(operand_it->num_delete, operand_it->appended_segs);
    base = nb.Commit();
  }  // end for

  PreorderDump(base, &chunk_cacher);
  operands_.clear();
  all_operand_segs_.clear();
  CHECK(base.own());
  return base;
}

Chunk AdvancedNodeBuilder::ChunkCacher::GetChunk(const Hash& key) {
  auto cache_it = cache_.find(key);
  // read from write cache
  if (cache_it != cache_.end()) {
    return Chunk(cache_it->second);
  }
  // read from chunk loader
  return Chunk(loader_->Load(key)->head());
}
bool AdvancedNodeBuilder::ChunkCacher::ExistInCache(const Hash& key) const {
  auto cache_it = cache_.find(key);
  return cache_it != cache_.end();
}

bool AdvancedNodeBuilder::ChunkCacher::DumpCacheChunk(const Hash& key) {
  auto cache_it = cache_.find(key);
  if (cache_it != cache_.end()) {
    return writer_->Write(cache_it->first, Chunk(cache_it->second));
  } else {
    return false;
  }
}

bool AdvancedNodeBuilder::PreorderDump(const Hash& root, ChunkCacher* cacher) {
  bool result = true;
  if (cacher->ExistInCache(root)) {
    result = cacher->DumpCacheChunk(root);
    const Chunk* chunk = cacher->Load(root);
    auto seq_node = SeqNode::CreateFromChunk(chunk);
    if (!seq_node->isLeaf()) {
      auto meta_node = dynamic_cast<const MetaNode*>(seq_node.get());
      for (size_t i = 0; i < meta_node->numEntries(); ++i) {
        Hash child_hash = meta_node->GetChildHashByEntry(i);
        result = result & PreorderDump(child_hash, cacher);
      }  // end for
    }  // end if seq_node
  }  // end if cacher
  return result;
}

}  // namespace ustore
