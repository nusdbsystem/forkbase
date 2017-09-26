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
        CHECK_EQ(0, this->idx_);
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

uint64_t NodeCursor::AdvanceSteps(uint64_t step) {
  uint64_t acc_step = 0;
  DLOG(INFO) << "\n";
  DLOG(INFO) << "Parameter Step: " << step;
  DLOG(INFO) << "!Before Parent: "
             << (seq_node_->isLeaf() ? "Leaf" : "Meta");
  DLOG(INFO) << "# Entry: " << seq_node_->numEntries();
  DLOG(INFO) << "  Before Index: " << idx_;
  if (seq_node_->isLeaf()) {
    uint64_t max_step = static_cast<uint64_t>(
                          static_cast<int32_t>(seq_node_->numEntries()) - idx_);

    // Only advance cursor within leaf node is enough
    if (step < max_step) {
      idx_ += static_cast<int32_t>(step);
      return step;
    } else {
      acc_step = max_step;
      idx_ = static_cast<int32_t>(seq_node_->numEntries());
    }
  } else {
    // internal node must point to a valid entry
    DCHECK(!isBegin() && !isEnd());

    uint64_t entry_num_elements = 0;
    ++idx_;
    while (!isEnd()) {
      MetaEntry meta_entry(current());
      entry_num_elements = meta_entry.numElements();
      if (acc_step + entry_num_elements > step) break;
      acc_step += entry_num_elements;
      ++idx_;
    }  // end while
    // DLOG(INFO) << "Meta Acc1 Step: "
    //            << acc_step;

    if (!isEnd()) {
      DCHECK_LT(step, acc_step + entry_num_elements);
      DCHECK_LE(acc_step, step);

      DLOG(INFO) << "Not End.  After Index: " << idx_;
      DLOG(INFO) << "Acc Step: " << acc_step;
      return acc_step;
    }  // end isEnd
  }  // end if

  DLOG(INFO) << "  After Index: " << idx_;
  DLOG(INFO) << "Acc Step: " << acc_step;
  DCHECK_LE(acc_step, step);
  DCHECK(isEnd());
  // Ask upper cursor for advance

  if (parent_cr_ == nullptr) {return acc_step; }

  uint64_t parent_step = parent_cr_->AdvanceSteps(step - acc_step);
  DLOG(INFO) << "\nAfter Parent: ";
  DLOG(INFO) << "  Parent Step: " << parent_step;

  DCHECK_LE(parent_step, step - acc_step);

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

    return acc_step + parent_step;
  }

  DCHECK_LE(acc_step + parent_step, step);
  uint64_t remain_step = step - acc_step - parent_step;

  uint64_t acc2_step = 0;

  if (seq_node_->isLeaf()) {
    DCHECK_LT(remain_step, seq_node_->numEntries());
    idx_ = static_cast<int32_t>(remain_step);
    DLOG(INFO) << "Leaf Final Cursor Position: " << idx_;
    acc2_step = remain_step;
  } else {
    idx_ = 0;
    while (!isEnd()) {
      MetaEntry meta_entry(current());
      uint64_t entry_num_elements = meta_entry.numElements();
      if (acc2_step + entry_num_elements > remain_step) break;
      acc2_step += entry_num_elements;
      ++idx_;
    }  // end while
    DLOG(INFO) << "Meta Final Cursor Position: " << idx_;

    DCHECK(!isEnd());
    DCHECK_LE(acc2_step, remain_step);
  }  // end if
  DLOG(INFO) << "acc2_step: " << acc2_step
             << "remain_step: " << remain_step;

  DLOG(INFO) << "Total Step: " << acc_step + parent_step + acc2_step;

  return acc_step + parent_step + acc2_step;
}

uint64_t NodeCursor::RetreatSteps(uint64_t step) {
  uint64_t acc_step = 0;
  DLOG(INFO) << "\n";
  DLOG(INFO) << "Parameter Step: " << step;
  DLOG(INFO) << "!Before Parent: "
             << (seq_node_->isLeaf() ? "Leaf" : "Meta");
  DLOG(INFO) << "# Entry: " << seq_node_->numEntries();
  DLOG(INFO) << "  Before Index: " << idx_;
  if (seq_node_->isLeaf()) {
    // Max step to advance to node start
    uint64_t max_step = static_cast<uint64_t>(idx_ + 1);

    // Only advance cursor within leaf node is enough
    if (step < max_step) {
      idx_ -= static_cast<int32_t>(step);
      return step;
    } else {
      acc_step = max_step;
      idx_ = -1;
    }
  } else {
    // internal node must point to a valid entry
    DCHECK(!isBegin() && !isEnd());

    uint64_t entry_num_elements = 0;
    --idx_;
    while (!isBegin()) {
      MetaEntry meta_entry(current());
      entry_num_elements = meta_entry.numElements();
      if (acc_step + entry_num_elements > step) break;
      acc_step += entry_num_elements;
      --idx_;
    }  // end while

    if (!isBegin()) {
      DCHECK_LT(step, acc_step + entry_num_elements);
      DCHECK_LE(acc_step, step);

      DLOG(INFO) << "Not Begin.  After Index: " << idx_;
      DLOG(INFO) << "Acc Step: " << acc_step;
      return acc_step;
    }  // end isEnd
  }  // end if

  DLOG(INFO) << "  After Index: " << idx_;
  DLOG(INFO) << "Acc Step: " << acc_step;
  DCHECK_LE(acc_step, step);
  DCHECK(isBegin());
  // Ask upper cursor for advance

  if (parent_cr_ == nullptr) {return acc_step; }

  uint64_t parent_step = parent_cr_->RetreatSteps(step - acc_step);
  DLOG(INFO) << "\nAfter Parent: ";
  DLOG(INFO) << "  Parent Step: " << parent_step;
  DCHECK_LE(parent_step, step - acc_step);

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
    return acc_step + parent_step;
  }

  DCHECK_LE(acc_step + parent_step, step);
  uint64_t remain_step = step - acc_step - parent_step;
  uint64_t acc2_step = 0;

  if (seq_node_->isLeaf()) {
    DCHECK_LT(remain_step, seq_node_->numEntries());
    idx_ = static_cast<int32_t>(seq_node_->numEntries()) - 1
           - static_cast<int32_t>(remain_step);
    DLOG(INFO) << "Leaf Final Cursor Position: " << idx_;
    acc2_step = remain_step;
  } else {
    idx_ = static_cast<int32_t>(seq_node_->numEntries()) - 1;
    while (!isBegin()) {
      MetaEntry meta_entry(current());
      uint64_t entry_num_elements = meta_entry.numElements();
      if (acc2_step + entry_num_elements > remain_step) break;
      acc2_step += entry_num_elements;
      --idx_;
    }  // end while
    DLOG(INFO) << "Meta Final Cursor Position: " << idx_;
    DCHECK(!isEnd());
    DCHECK_LE(acc2_step, remain_step);
  }  // end if
  DLOG(INFO) << "acc2_step: " << acc2_step
             << "remain_step: " << remain_step;
  DLOG(INFO) << "Total Step: " << acc_step + parent_step + acc2_step;

  return acc_step + parent_step + acc2_step;
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
