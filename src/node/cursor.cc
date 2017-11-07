// Copyright (c) 2017 The Ustore Authors.

#include "node/cursor.h"

#include "hash/hash.h"
#include "node/blob_node.h"
#include "node/list_node.h"
#include "node/map_node.h"
#include "node/node.h"
#include "node/orderedkey.h"
#include "utils/logging.h"

namespace ustore {
std::vector<IndexRange> IndexRange::Compact(
    const std::vector<IndexRange>& ranges) {
  if (ranges.size() == 0) return ranges;
  std::vector<IndexRange> result;
  IndexRange curr_cr = ranges[0];
  uint64_t pre_upper = curr_cr.start_idx + curr_cr.num_subsequent;

  for (size_t i = 1; i < ranges.size(); ++i) {
    CHECK_NE(ranges[i].num_subsequent, size_t(0));
    if (pre_upper < ranges[i].start_idx) {
      result.push_back(curr_cr);
      curr_cr = ranges[i];
    } else if (pre_upper == ranges[i].start_idx) {
      curr_cr.num_subsequent += ranges[i].num_subsequent;
    } else {
      LOG(FATAL) << "The upper bound of last Index Range"
                 << " is greater than the lower bound of current Index Range.";
    }

    pre_upper = ranges[i].start_idx + ranges[i].num_subsequent;
  }

  result.push_back(curr_cr);
  return result;
}

std::vector<std::pair<IndexRange, IndexRange>> IndexRange::Compact(
    const std::vector<std::pair<IndexRange, IndexRange>>& range_maps) {
  if (range_maps.size() == 0) return range_maps;
  std::vector<std::pair<IndexRange, IndexRange>> result;

  IndexRange cur_lhs_range = range_maps[0].first;
  IndexRange cur_rhs_range = range_maps[0].second;

  uint64_t pre_lhs_upper = cur_lhs_range.start_idx
                           + cur_lhs_range.num_subsequent;
  uint64_t pre_rhs_upper = cur_rhs_range.start_idx
                           + cur_rhs_range.num_subsequent;

  for (size_t i = 1; i < range_maps.size(); ++i) {
    IndexRange lhs_range = range_maps[i].first;
    IndexRange rhs_range = range_maps[i].second;

    DCHECK_NE(lhs_range.num_subsequent, size_t(0));
    DCHECK_EQ(lhs_range.num_subsequent, rhs_range.num_subsequent);

    if (pre_lhs_upper < lhs_range.start_idx ||
        pre_rhs_upper < rhs_range.start_idx) {
      result.push_back({cur_lhs_range, cur_rhs_range});
      cur_lhs_range = lhs_range;
      cur_rhs_range = rhs_range;
    } else if (pre_lhs_upper == lhs_range.start_idx &&
               pre_rhs_upper == rhs_range.start_idx) {
      cur_lhs_range.num_subsequent += lhs_range.num_subsequent;
      cur_rhs_range.num_subsequent += rhs_range.num_subsequent;
    } else {
      LOG(FATAL) << "Invalid Index Range Maps. ";
    }

    pre_lhs_upper = cur_lhs_range.start_idx + cur_lhs_range.num_subsequent;
    pre_rhs_upper = cur_rhs_range.start_idx + cur_rhs_range.num_subsequent;
  }

  result.push_back({cur_lhs_range, cur_rhs_range});
  return result;
}

NodeCursor::NodeCursor(const Hash& hash, size_t idx,
                       ChunkLoader* ch_loader) noexcept {
  NodeCursor* parent_cursor = nullptr;
  size_t element_idx = idx;
  size_t entry_idx = 0;
  const Chunk* chunk = ch_loader->Load(hash);

  std::shared_ptr<const SeqNode> seq_node(SeqNode::CreateFromChunk(chunk));
  if (idx <= seq_node->numElements()) {
    while (!seq_node->isLeaf()) {
      const MetaNode* mnode = dynamic_cast<const MetaNode*>(seq_node.get());
      Hash child_hash = mnode->GetChildHashByIndex(element_idx, &entry_idx);
      if (child_hash.empty()) {
        CHECK_EQ(entry_idx, mnode->numEntries());
        // idx exceeds the number of total elements
        //  child_hash = child hash of last entry
        //  entry_idx = idx of the last entry
        child_hash = mnode->GetChildHashByEntry(mnode->numEntries() - 1);
        entry_idx = mnode->numEntries() - 1;
      }
      parent_cursor = new NodeCursor(seq_node, entry_idx, ch_loader,
                            std::unique_ptr<NodeCursor>(parent_cursor));
      element_idx -= mnode->numElementsUntilEntry(entry_idx);
      chunk = ch_loader->Load(child_hash);
      seq_node = SeqNode::CreateFromChunk(chunk);
    }
    // if the element_idx > num of elements at leaf
    //   make cursor point to the end of leaf
    //   entry_idx = numEntries()
    const LeafNode* lnode = dynamic_cast<const LeafNode*>(seq_node.get());
    if (element_idx > lnode->numEntries()) {
      entry_idx = lnode->numElements();
    } else {
      entry_idx = element_idx;
    }
    seq_node_ = seq_node;
    idx_ = entry_idx;
    chunk_loader_ = ch_loader;
    parent_cr_.reset(parent_cursor);
  } else {
    // idx > # of total elements
    // do nothing
  }
}

NodeCursor::NodeCursor(const Hash& hash, const OrderedKey& key,
                       ChunkLoader* ch_loader) noexcept {
  NodeCursor* parent_cursor = nullptr;
  size_t entry_idx = 0;
  const Chunk* chunk = ch_loader->Load(hash);
  std::shared_ptr<const SeqNode> seq_node(SeqNode::CreateFromChunk(chunk));

  while (!seq_node->isLeaf()) {
    const MetaNode* mnode = dynamic_cast<const MetaNode*>(seq_node.get());
    Hash child_hash = mnode->GetChildHashByKey(key, &entry_idx);
    if (child_hash.empty()) {
      CHECK_EQ(entry_idx, mnode->numEntries());
      // idx exceeds the number of total elements
      //  child_hash = child hash of last entry
      //  entry_idx = idx of the last entry
      child_hash = mnode->GetChildHashByEntry(mnode->numEntries() - 1);
      entry_idx = mnode->numEntries() - 1;
    }
    parent_cursor = new NodeCursor(seq_node, entry_idx, ch_loader,
                          std::unique_ptr<NodeCursor>(parent_cursor));
    chunk = ch_loader->Load(child_hash);
    seq_node = SeqNode::CreateFromChunk(chunk);
  }
  // if the element_idx > num of elements at leaf
  //   make cursor point to the end of leaf
  //   entry_idx = numEntries()
  const LeafNode* lnode = dynamic_cast<const LeafNode*>(seq_node.get());
  entry_idx = lnode->FindIndexForKey(key, ch_loader);

  seq_node_ = seq_node;
  idx_ = entry_idx;
  chunk_loader_ = ch_loader;
  parent_cr_.reset(parent_cursor);
}

// copy ctor
NodeCursor::NodeCursor(const NodeCursor& cursor) noexcept
    : parent_cr_(nullptr),
      seq_node_(cursor.seq_node_),
      chunk_loader_(cursor.chunk_loader_),
      idx_(cursor.idx_) {
  if (cursor.parent_cr_) {
    this->parent_cr_.reset(new NodeCursor(*(cursor.parent_cr_)));
  }
}

NodeCursor::NodeCursor(std::shared_ptr<const SeqNode> seq_node, size_t idx,
                       ChunkLoader* chunk_loader,
                       std::unique_ptr<NodeCursor> parent_cr)
    : parent_cr_(std::move(parent_cr)),
      seq_node_(seq_node),
      chunk_loader_(chunk_loader),
      idx_(idx) {
  // do nothing
}

bool NodeCursor::Advance(bool cross_boundary) {
  // DLOG(INFO) << "Before Advance: idx = " << idx_;
  // DLOG(INFO) << "  numEntries = " << seq_node_->numEntries();
  // DLOG(INFO) << "  isLeaf = " << seq_node_->isLeaf();
  // when idx == -1, idx_ < seq_node->numEntries is false.
  //  This is because lhs is signed and rhs is not signed.
  //  Hence, add extra test on idx_ == -1
  if (idx_ == -1 || idx_ < int32_t(seq_node_->numEntries())) ++idx_;
  if (idx_ < int32_t(seq_node_->numEntries())) return true;
  DCHECK_EQ(size_t(idx_), seq_node_->numEntries());
  // not allow to cross boundary,
  //   remain idx = numEntries()
  if (!cross_boundary) return false;
  // current cursor points to the seq end (idx = numEntries())
  //   will be retreated by child cursor
  if (parent_cr_ == nullptr) return false;
  if (parent_cr_->Advance(true)) {
    MetaEntry me(parent_cr_->current());
    const Chunk* chunk = chunk_loader_->Load(me.targetHash());
    seq_node_ = SeqNode::CreateFromChunk(chunk);
    DCHECK_GT(seq_node_->numEntries(), size_t(0));
    idx_ = 0;  // point the first element
    return true;
  } else {
    // curent parent cursor now points the seq end, (idx = numEntries())
    //   must retreat to point to the last element (idx = numEntries() - 1)
    parent_cr_->Retreat(false);
    return false;
  }
}

bool NodeCursor::Retreat(bool cross_boundary) {
  if (idx_ >= 0) --idx_;
  if (idx_ >= 0) return true;
  DCHECK_EQ(idx_, int32_t(-1));
  // not allow to cross boundary,
  //   remain idx = -1
  if (!cross_boundary) return false;
  // remain idx = -1, to be advanced by child cursor.
  if (parent_cr_ == nullptr) return false;
  if (parent_cr_->Retreat(true)) {
    MetaEntry me(parent_cr_->current());
    const Chunk* chunk = chunk_loader_->Load(me.targetHash());
    seq_node_ = SeqNode::CreateFromChunk(chunk);
    DCHECK_GT(seq_node_->numEntries(), size_t(0));
    idx_ = seq_node_->numEntries() - 1;  // point to the last element
    return true;
  } else {
    // parent cursor now points the seq start, (idx = -1)
    //   must advance to point to the frist element (idx = 0)
    parent_cr_->Advance(false);
    return false;
  }
}

size_t NodeCursor::AdvanceEntry(size_t num_advanced_entry) {
  // Attemp to advance to a valid entry. If failed, return 0
  if (isEnd() && !this->Advance(true)) return 0;
  size_t node_num_entry = this->node()->numEntries();
  size_t node_remain_entry = static_cast<size_t>(
      static_cast<int32_t>(node_num_entry) - this->idx_);

  // Accumulate entries advanced
  size_t remain_advanced_entry = num_advanced_entry;
  bool isSeqEnd = false;
  do {
    if (node_remain_entry <= remain_advanced_entry) {
      this->idx_ += static_cast<int32_t>(node_remain_entry) - 1;
      isSeqEnd = !this->Advance(true);
      remain_advanced_entry -= node_remain_entry;
      if (!isSeqEnd) {
        CHECK_EQ(int32_t(0), this->idx_);
        node_num_entry = this->node()->numEntries();
        node_remain_entry = node_num_entry;
      }
    } else {
      this->idx_ += static_cast<int32_t>(remain_advanced_entry);
      CHECK_LT(this->idx_, static_cast<int32_t>(this->node()->numEntries()));
      remain_advanced_entry = 0;
    }
  } while (!isSeqEnd && remain_advanced_entry > 0);
  return num_advanced_entry - remain_advanced_entry;
}

size_t NodeCursor::RetreatEntry(size_t num_retreat_entry) {
  // Attemp to retreat to a valid entry. If failed, return 0
  if (isBegin() && !this->Retreat(true)) return 0;

  size_t node_num_entry = this->node()->numEntries();
  size_t node_remain_entry = static_cast<size_t>(this->idx_);

  // Accumulate entries retreat
  size_t remain_retreat_entry = num_retreat_entry;
  bool isSeqBegin = false;
  do {
    if (node_remain_entry < remain_retreat_entry) {
      this->idx_ = 0;
      isSeqBegin = !this->Retreat(true);
      remain_retreat_entry -= node_remain_entry + 1;
      if (!isSeqBegin) {
        CHECK_EQ(this->node()->numEntries() - 1, size_t(this->idx_));
        node_num_entry = this->node()->numEntries();
        node_remain_entry = node_num_entry - 1;
      }
    } else {
      this->idx_ -= static_cast<int32_t>(remain_retreat_entry);
      CHECK_LE(0, this->idx_);
      remain_retreat_entry = 0;
    }
  } while (!isSeqBegin && remain_retreat_entry > 0);
  return num_retreat_entry - remain_retreat_entry;
}

/* Cursor makes the advancement on three phases

1. Move steps and advance to current chunk last element.
2. Recursively call parent cursor to move for the remaining steps.
3. Move the remaining steps and advance to the element
*/
uint64_t NodeCursor::AdvanceSteps(uint64_t step) {
  uint64_t acc1_step = 0;  // Accumulated steps in Phase 1
  // DLOG(INFO) << "\n";
  // DLOG(INFO) << "Parameter Step: " << step;
  // DLOG(INFO) << "!Before Parent: "
  //            << (seq_node_->isLeaf() ? "Leaf" : "Meta");
  // DLOG(INFO) << "# Entry: " << seq_node_->numEntries();
  // DLOG(INFO) << "  Before Index: " << idx_;
  bool initial_end = isEnd();
  if (!initial_end) {
    if (seq_node_->isLeaf()) {
      uint64_t step2last = static_cast<uint64_t>(
                            static_cast<int32_t>(seq_node_->numEntries()) - 1 - idx_);
      // Only advance cursor within leaf node is enough
      if (step <= step2last) {
        idx_ += static_cast<int32_t>(step);
        return step;
      } else {
        acc1_step = step2last;
        idx_ = static_cast<int32_t>(seq_node_->numEntries());
      }  // end if (step <= step2last)
    } else {
      uint64_t entry_num_elements = 0;
      uint64_t last_entry_num_elements = 0;

      // Remaining steps to advance in Phase 1
      int64_t remain1_steps = static_cast<int64_t>(step);

      while (remain1_steps > 0) {
        ++idx_;
        acc1_step += last_entry_num_elements;
        if (isEnd()) { break; }
        MetaEntry meta_entry(current());
        entry_num_elements = meta_entry.numElements();

        remain1_steps -= static_cast<int64_t>(entry_num_elements);
        last_entry_num_elements = entry_num_elements;
      }  // end while
      // DLOG(INFO) << "Meta Acc1 Step: "
      //            << acc1_step;

      if (remain1_steps <= 0) {
        // Current metanode can already accomodate the advanced steps.
        DCHECK_LE(step, acc1_step + entry_num_elements);
        DCHECK_LE(acc1_step, step);

        // DLOG(INFO) << "Not End.  After Index: " << idx_;
        // DLOG(INFO) << "Acc Step: " << acc1_step;
        return acc1_step;
      }  // end isEnd
    }  // end if
  }  // end if End()

  // DLOG(INFO) << "  After Index: " << idx_;
  // DLOG(INFO) << "Acc Step: " << acc1_step;
  DCHECK(isEnd());
  DCHECK_LE(acc1_step, step);
  // Ask upper cursor for advance

  if (parent_cr_ == nullptr) {
    return acc1_step + (initial_end ? 0 : 1);
  }

  uint64_t parent_step = parent_cr_->AdvanceSteps(step - acc1_step);
  // DLOG(INFO) << "\nAfter Parent: ";
  // DLOG(INFO) << "  Parent Step: " << parent_step;

  bool endParent = parent_cr_->isEnd();
  if (endParent) {
    // Make parent cursor points to last valid metaentry
    bool notBegin = parent_cr_->Retreat(false);
    DCHECK(notBegin);
  }

  // Load this cursor seqnode from the entry pointed by parent cursor
  MetaEntry me(parent_cr_->current());
  seq_node_ = SeqNode::CreateFromChunk(chunk_loader_->Load(me.targetHash()));
  DCHECK_GT(seq_node_->numEntries(), size_t(0));

  if (endParent) {
    // Place this cursor to seq end
    idx_ = static_cast<int32_t>(seq_node_->numEntries());
    return acc1_step + parent_step;
  }

  DCHECK_LE(acc1_step + parent_step, step);

  // Remaining steps to advance in Phase 3
  uint64_t remain3_step = step - acc1_step - parent_step;

  // Accumulated advanced steps in Phase 3
  uint64_t acc3_step = 0;

  if (seq_node_->isLeaf()) {
    DCHECK_LE(remain3_step, seq_node_->numEntries());
    idx_ = static_cast<int32_t>(remain3_step - 1);
    // DLOG(INFO) << "Leaf Final Cursor Position: " << idx_;
    acc3_step = remain3_step;
  } else {
    idx_ = 0;
    while (!isEnd()) {
      MetaEntry meta_entry(current());
      uint64_t entry_num_elements = meta_entry.numElements();
      if (acc3_step + entry_num_elements > remain3_step) break;
      acc3_step += entry_num_elements;
      ++idx_;
    }  // end while
    // DLOG(INFO) << "Meta Final Cursor Position: " << idx_;

    DCHECK_LE(acc3_step, remain3_step);
  }  // end if
  // DLOG(INFO) << "acc3_step: " << acc3_step
             // << "remain_step: " << remain_step;

  // DLOG(INFO) << "Total Step: " << acc_step + parent_step + acc3_step;

  return acc1_step + parent_step + acc3_step;
}

/* Cursor makes the retreatment on three phases

1. Move steps and retreat to current chunk first element.
2. Recursively call parent cursor to move for the remaining steps.
3. Move the remaining steps and retreat to the final element
*/
uint64_t NodeCursor::RetreatSteps(uint64_t step) {
  // Accumulated retreated steps in Phase 1
  uint64_t acc1_step = 0;
  // DLOG(INFO) << "\n";
  // DLOG(INFO) << "Parameter Step: " << step;
  // DLOG(INFO) << "!Before Parent: "
             // << (seq_node_->isLeaf() ? "Leaf" : "Meta");
  // DLOG(INFO) << "# Entry: " << seq_node_->numEntries();
  // DLOG(INFO) << "  Before Index: " << idx_;
  bool initial_begin = isBegin();
  if (!initial_begin){
    if (seq_node_->isLeaf()) {
      // Max step to advance to the first element
      uint64_t step2first = static_cast<uint64_t>(idx_);

      // Only advance cursor within leaf node is enough
      if (step <= step2first) {
        idx_ -= static_cast<int32_t>(step);
        return step;
      } else {
        acc1_step = step2first;
        idx_ = -1;
      }  // end if step <= step2first
    } else {

      uint64_t entry_num_elements = 0;
      uint64_t prev_entry_num_elements = 0;

      // Remaining steps to advance in Phase 1
      int64_t remain1_steps = static_cast<int64_t>(step);

      while (remain1_steps > 0) {
        --idx_;
        acc1_step += prev_entry_num_elements;
        if (isBegin()) {break; }
        MetaEntry meta_entry(current());
        entry_num_elements = meta_entry.numElements();

        remain1_steps -= static_cast<int64_t>(entry_num_elements);
        prev_entry_num_elements = entry_num_elements;
      }  // end while

      if (remain1_steps <= 0) {
        DCHECK_LE(step, acc1_step + entry_num_elements);
        DCHECK_LE(acc1_step, step);

        // DLOG(INFO) << "Not Begin.  After Index: " << idx_;
        // DLOG(INFO) << "Acc Step: " << acc1_step;
        return acc1_step;
      }  // end isEnd
    }  // end if
  }  // end if isBegin()

  // DLOG(INFO) << "  After Index: " << idx_;
  // DLOG(INFO) << "Acc Step: " << acc1_step;
  DCHECK_LE(acc1_step, step);
  DCHECK(isBegin());
  // Ask upper cursor for advance

  if (parent_cr_ == nullptr) {
    return acc1_step + (initial_begin ? 0 : 1);
  }

  uint64_t parent_step = parent_cr_->RetreatSteps(step - acc1_step);
  // DLOG(INFO) << "\nAfter Parent: ";
  // DLOG(INFO) << "  Parent Step: " << parent_step;

  bool headParent = parent_cr_->isBegin();
  if (headParent) {
    // Make parent cursor points to first valid metaentry
    bool notEnd = parent_cr_->Advance(false);
    DCHECK(notEnd);
  }

  // Load this cursor seqnode from the entry pointed by parent cursor
  MetaEntry me(parent_cr_->current());
  seq_node_ = SeqNode::CreateFromChunk(chunk_loader_->Load(me.targetHash()));
  DCHECK_GT(seq_node_->numEntries(), size_t(0));

  if (headParent) {
    // Place this cursor to seq head
    idx_ = -1;
    return acc1_step + parent_step;
  }

  DCHECK_LT(acc1_step + parent_step, step);
  // Remaining steps to advance in Phase 3
  uint64_t remain3_step = step - acc1_step - parent_step;
  // Accumulated retreated steps in Phase 3
  uint64_t acc3_step = 0;

  if (seq_node_->isLeaf()) {
    DCHECK_LE(remain3_step, seq_node_->numEntries());
    idx_ = static_cast<int32_t>(seq_node_->numEntries())
           - static_cast<int32_t>(remain3_step);
    // DLOG(INFO) << "Leaf Final Cursor Position: " << idx_;
    acc3_step = remain3_step;
  } else {
    // DLOG(INFO) << "Meta Final Cursor Position: " << idx_;

    idx_ = static_cast<int32_t>(seq_node_->numEntries()) - 1;
    while (!isBegin()) {
      MetaEntry meta_entry(current());
      uint64_t entry_num_elements = meta_entry.numElements();
      if (acc3_step + entry_num_elements > remain3_step) break;
      acc3_step += entry_num_elements;
      --idx_;
    }  // end while
    // DLOG(INFO) << "Meta Final Cursor Position: " << idx_;
    DCHECK_LE(acc3_step, remain3_step);
  }  // end if
  // DLOG(INFO) << "acc3_step: " << acc3_step
             // << "remain_step: " << remain_step;
  // DLOG(INFO) << "Total Step: " << acc1_step + parent_step + acc3_step;

  return acc1_step + parent_step + acc3_step;
}

const byte_t* NodeCursor::current() const {
  if (idx_ == -1) {
    LOG(WARNING) << "Cursor points to Seq Head. Return nullptr.";
    return nullptr;
  }
  if (idx_ == int32_t(seq_node_->numEntries())) {
    DLOG(WARNING) << "Cursor points to Seq End. Return pointer points to byte "
                     "after the last entry.";
    return seq_node_->data(idx_ - 1) + seq_node_->len(idx_ - 1);
  }
  return seq_node_->data(idx_);
}

size_t NodeCursor::numCurrentBytes() const {
  if (idx_ == -1) {
    LOG(WARNING) << "Cursor points to Seq Head. Return 0.";
    return 0;
  }
  if (idx_ == int32_t(seq_node_->numEntries())) {
    LOG(WARNING) << "Cursor points to Seq End. Return 0.";
    return 0;
  }
  return seq_node_->len(idx_);
}

// two cursor are equal if the following condition are ALL met:
//   same idx
//   point to the same seqnode
//   same parent cursor
bool NodeCursor::operator==(const NodeCursor& rhs) const {
  bool equal = this->idx_ == rhs.idx_ &&
               this->seq_node_->hash() ==
               rhs.seq_node_->hash();
  if (!equal) return false;

  if (this->parent_cr_ == nullptr &&
      rhs.parent_cr_ == nullptr) {
    return true;
  } else if (this->parent_cr_ == nullptr ||
             rhs.parent_cr_ == nullptr) {
    return false;
  } else {
    return *this->parent_cr_ == *rhs.parent_cr_;
  }
}

}  // namespace ustore
