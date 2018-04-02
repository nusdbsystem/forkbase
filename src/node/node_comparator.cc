// Copyright (c) 2017 The Ustore Authors.
#include <algorithm>
#ifdef DEBUG
#include <iomanip>
#include <sstream>
#include <string>
#endif

#include "node/node_comparator.h"

namespace ustore {
RangeMaps LevenshteinMapper::Compare(const ustore::Hash &lhs,
                                     ChunkLoader* lloader) const {
  if (lloader == nullptr) {
    lloader = rloader_;
  }
  const Chunk* lhs_chunk = lloader->Load(lhs);
  const Chunk* rhs_chunk = rloader_->Load(rhs_);

  auto lhs_node = SeqNode::CreateFromChunk(lhs_chunk);
  auto rhs_node = SeqNode::CreateFromChunk(rhs_chunk);

  uint64_t lhs_num_elements = lhs_node->numElements();
  uint64_t rhs_num_elements = rhs_node->numElements();

  if (lhs_num_elements == 0 || rhs_num_elements == 0) {
    return {};
  }

  std::unique_ptr<NodeCursor> lhs_cr(new NodeCursor(lhs, 0, lloader));
  std::unique_ptr<NodeCursor> rhs_cr(new NodeCursor(rhs_, 0, rloader_));

  return map(std::move(lhs_cr), {0, lhs_num_elements},
             std::move(rhs_cr), {0, rhs_num_elements});
}

RangeMaps LevenshteinMapper::map(
    std::unique_ptr<NodeCursor> lhs_cr, IndexRange lhs_range,
    std::unique_ptr<NodeCursor> rhs_cr, IndexRange rhs_range) const {
  RangeMaps range_maps;
  if (lhs_cr->parent() && rhs_cr->parent()) {
    // Copy parent cursor for both LHS and RHS
    std::unique_ptr<NodeCursor> lhs_parent_cr(
        new NodeCursor(*lhs_cr->parent()));
    std::unique_ptr<NodeCursor> rhs_parent_cr(
        new NodeCursor(*rhs_cr->parent()));

    RangeMaps upper_maps = map(std::move(lhs_parent_cr), lhs_range,
                               std::move(rhs_parent_cr), rhs_range);

    DLOG(INFO) << "===========================================================";
    uint64_t lhs_last_element_idx = 0;
    uint64_t rhs_last_element_idx = 0;
    for (std::pair<IndexRange, IndexRange> upper_map : upper_maps) {
      IndexRange lhs_range = upper_map.first;
      IndexRange rhs_range = upper_map.second;

      if (lhs_last_element_idx < lhs_range.start_idx &&
          rhs_last_element_idx < rhs_range.start_idx) {
        IndexRange lhs_gap_range{lhs_last_element_idx,
                                 lhs_range.start_idx - lhs_last_element_idx};
        IndexRange rhs_gap_range{rhs_last_element_idx,
                                 rhs_range.start_idx - rhs_last_element_idx};
#ifdef DEBUG
        DLOG(INFO) << "Gap Range: "
                   << IndexRange::to_str({lhs_gap_range, rhs_gap_range});
#endif
        // Copy current cursor for both LHS and RHS
        std::unique_ptr<NodeCursor> lhs_cr_cpy(new NodeCursor(*lhs_cr));
        std::unique_ptr<NodeCursor> rhs_cr_cpy(new NodeCursor(*rhs_cr));

        uint64_t lhs_actual_steps =
            lhs_cr_cpy->AdvanceSteps(lhs_last_element_idx);
        DCHECK_EQ(lhs_actual_steps, lhs_last_element_idx);

        uint64_t rhs_actual_steps =
            rhs_cr_cpy->AdvanceSteps(rhs_last_element_idx);
        DCHECK_EQ(rhs_actual_steps, rhs_last_element_idx);

        RangeMaps cur_map = SeqMap(std::move(lhs_cr_cpy), lhs_gap_range,
                                   std::move(rhs_cr_cpy), rhs_gap_range);

        range_maps.insert(range_maps.end(), cur_map.begin(), cur_map.end());
      }  // end if (lhs_last_element_idx < lhs_range.start_idx

#ifdef DEBUG
      DLOG(INFO) << "Upper Range: "
                 << IndexRange::to_str(upper_map);
#endif
      range_maps.push_back(upper_map);
      lhs_last_element_idx = lhs_range.start_idx + lhs_range.num_subsequent;
      rhs_last_element_idx = rhs_range.start_idx + rhs_range.num_subsequent;
    }  // end for

    uint64_t lhs_end_element_idx = lhs_range.start_idx
                                   + lhs_range.num_subsequent;

    uint64_t rhs_end_element_idx = rhs_range.start_idx
                                   + rhs_range.num_subsequent;

    if (lhs_last_element_idx < lhs_end_element_idx &&
        rhs_last_element_idx < rhs_end_element_idx) {
      IndexRange lhs_gap_range{lhs_last_element_idx,
                               lhs_end_element_idx - lhs_last_element_idx};
      IndexRange rhs_gap_range{rhs_last_element_idx,
                               rhs_end_element_idx - rhs_last_element_idx};

      // Copy current cursor for both LHS and RHS
      std::unique_ptr<NodeCursor> lhs_cr_cpy(new NodeCursor(*lhs_cr));
      std::unique_ptr<NodeCursor> rhs_cr_cpy(new NodeCursor(*rhs_cr));
#ifdef DEBUG
      DLOG(INFO) << "Final Gap Range: "
                 << IndexRange::to_str({lhs_gap_range, rhs_gap_range});
#endif
      uint64_t lhs_actual_steps =
          lhs_cr_cpy->AdvanceSteps(lhs_last_element_idx);
      CHECK_EQ(lhs_actual_steps, lhs_last_element_idx);

      uint64_t rhs_actual_steps =
          rhs_cr_cpy->AdvanceSteps(rhs_last_element_idx);
      CHECK_EQ(rhs_actual_steps, rhs_last_element_idx);

      DCHECK_EQ(0, lhs_cr_cpy->idx());
      DCHECK_EQ(0, rhs_cr_cpy->idx());

      RangeMaps cur_map = SeqMap(std::move(lhs_cr_cpy), lhs_gap_range,
                                 std::move(rhs_cr_cpy), rhs_gap_range);

      range_maps.insert(range_maps.end(), cur_map.begin(), cur_map.end());
    }  // end if (lhs_last_element_idx < lhs_range.start_idx &&
  } else {
    DLOG(INFO) << "===========================================================";
    range_maps = SeqMap(std::move(lhs_cr), lhs_range,
                        std::move(rhs_cr), rhs_range);
  }  // end if (lhs_cr->parent() && rhs_cr->parent())
  return IndexRange::Compact(range_maps);
}

RangeMaps LevenshteinMapper::SeqMap(
    std::unique_ptr<NodeCursor> lhs_cr, IndexRange lhs_range,
    std::unique_ptr<NodeCursor> rhs_cr, IndexRange rhs_range) const {
// It is impossible that lhs_cur or rhs_cr point to objects with no elements!
  RangeMaps result;

#ifdef DEBUG
  DLOG(INFO) << "Perform Sequence Map on "
             << IndexRange::to_str({lhs_range, rhs_range});
  NodeCursor lhs_cr_cpy(*lhs_cr);
  NodeCursor rhs_cr_cpy(*rhs_cr);

  DCHECK_EQ(0, lhs_cr->idx());
  DCHECK_EQ(0, rhs_cr->idx());
#endif

  std::vector<uint64_t> lhs_entry_elements;
  std::vector<uint64_t> rhs_entry_elements;

  std::vector<uint64_t> lhs_prefix_acc_elements;
  std::vector<uint64_t> rhs_prefix_acc_elements;

  uint64_t lhs_acc_count = 0;
  while (lhs_acc_count < lhs_range.num_subsequent) {
    lhs_prefix_acc_elements.push_back(lhs_acc_count);
    uint64_t numCursorElements = numElementsByCursor(*lhs_cr);
    lhs_entry_elements.push_back(numCursorElements);
    lhs_acc_count += numCursorElements;
    bool isSeqEnd = !lhs_cr->Advance(true);
    DCHECK(!isSeqEnd || lhs_acc_count == lhs_range.num_subsequent)
      << "is Seq End: " << isSeqEnd << " lhs_acc_count: " << lhs_acc_count
      << " lhs_range_num_subsequent: " << lhs_range.num_subsequent;
  }
  CHECK_EQ(lhs_acc_count, lhs_range.num_subsequent);

  uint64_t rhs_acc_count = 0;
  while (rhs_acc_count < rhs_range.num_subsequent) {
    rhs_prefix_acc_elements.push_back(rhs_acc_count);
    uint64_t numCursorElements = numElementsByCursor(*rhs_cr);
    rhs_entry_elements.push_back(numCursorElements);
    rhs_acc_count += numCursorElements;
    bool isSeqEnd = !rhs_cr->Advance(true);
    DCHECK(!isSeqEnd || rhs_acc_count == rhs_range.num_subsequent);
  }
  CHECK_EQ(rhs_acc_count, rhs_range.num_subsequent);

  size_t lhs_entry_count = lhs_entry_elements.size();
  size_t rhs_entry_count = rhs_entry_elements.size();

  size_t lhs_entry_retreat = lhs_cr->RetreatEntry(lhs_entry_count);
  size_t rhs_entry_retreat = rhs_cr->RetreatEntry(rhs_entry_count);

  CHECK_EQ(lhs_entry_retreat, lhs_entry_count);
  CHECK_EQ(rhs_entry_retreat, rhs_entry_count);
#ifdef DEBUG
  DCHECK(lhs_cr_cpy == *lhs_cr);
  DCHECK(rhs_cr_cpy == *rhs_cr);

  std::ostringstream lhs_entry_elements_str;
  std::ostringstream rhs_entry_elements_str;
  std::ostringstream lhs_prefix_acc_elements_str;
  std::ostringstream rhs_prefix_acc_elements_str;

  for (size_t i = 0; i < lhs_entry_count; ++i) {
    lhs_entry_elements_str << lhs_entry_elements[i] << ", ";
    lhs_prefix_acc_elements_str << lhs_prefix_acc_elements[i] << ", ";
  }

  for (size_t j = 0; j < rhs_entry_count; ++j) {
    rhs_entry_elements_str << rhs_entry_elements[j] << ", ";
    rhs_prefix_acc_elements_str << rhs_prefix_acc_elements[j] << ", ";
  }

  DLOG(INFO) << "LHS Entry Elements: " << lhs_entry_elements_str.str();
  DLOG(INFO) << "RHS Entry Elements: " << rhs_entry_elements_str.str();
  DLOG(INFO) << "LHS Prefix Entry Elements: "
             << lhs_prefix_acc_elements_str.str();
  DLOG(INFO) << "RHS Prefix Entry Elements: "
             << rhs_prefix_acc_elements_str.str();
#endif

// Construct the edit matrix based on Levenshtein Formular
  size_t num_row = static_cast<size_t>(lhs_entry_count + 1);
  size_t num_col = static_cast<size_t>(rhs_entry_count + 1);
  Entry* edit_matrix = new Entry[(num_row) * (num_col)];

  *edit_matrix = {EditMarker::kNull, 0};  // Entry 0, 0
  for (size_t i = 1; i <= lhs_entry_count; ++i) {
    Entry* pre_entry = edit_matrix + (i - 1) * num_col;  // Entry (i-1, 0)
    Entry* cur_entry = edit_matrix + i * num_col;  // Entry (i, 0)
    cur_entry->marker = EditMarker::kLHSInsertion;
    cur_entry->edit_distance = pre_entry->edit_distance + 1;
  }

  for (size_t j = 1; j <= rhs_entry_count; ++j) {
    Entry* pre_entry = edit_matrix + j - 1;  // Entry (0, j-1)
    Entry* cur_entry = edit_matrix + j;  // Entry (0, j)
    cur_entry->marker = EditMarker::kRHSInsertion;
    cur_entry->edit_distance = pre_entry->edit_distance + 1;
  }

  for (size_t i = 1; i <= lhs_entry_count; ++i) {
    for (size_t j = 1; j <= rhs_entry_count; ++j) {
      Entry* cur_entry = edit_matrix + i * num_col + j;  // Entry (i, j)
      cur_entry->marker = EditMarker::kNull;
      cur_entry->edit_distance = std::max(lhs_entry_count,
                                          rhs_entry_count) + 1;
      Entry* left_entry = edit_matrix + i * num_col  + j - 1;  // Entry (i, j-1)
      uint64_t achievable_distance = left_entry->edit_distance + 1;

      if (achievable_distance <= cur_entry->edit_distance) {
        cur_entry->marker = EditMarker::kRHSInsertion;
        cur_entry->edit_distance = achievable_distance;
      }  // end if

      Entry* up_entry = edit_matrix + (i - 1) * num_col  + j;  // Entry (i-1, j)
      achievable_distance = up_entry->edit_distance + 1;
      if (achievable_distance <= cur_entry->edit_distance) {
        cur_entry->marker = EditMarker::kLHSInsertion;
        cur_entry->edit_distance = achievable_distance;
      }  // end if

      // Diagonal Entry (i-1, j-1)
      size_t offset = (i - 1) * num_col  + (j - 1);
      Entry* dgn_entry = edit_matrix + offset;
      if (lhs_cr->numCurrentBytes() == rhs_cr->numCurrentBytes() &&
          std::memcmp(lhs_cr->current(), rhs_cr->current(),
                      lhs_cr->numCurrentBytes()) == 0) {
        achievable_distance = dgn_entry->edit_distance;
        if (achievable_distance <= cur_entry->edit_distance) {
          cur_entry->marker = EditMarker::kMatch;
          cur_entry->edit_distance = achievable_distance;
        }  // end if
      } else {
        achievable_distance = achievable_distance + 1;
        achievable_distance = dgn_entry->edit_distance + 1;
        if (achievable_distance <= cur_entry->edit_distance) {
          cur_entry->marker = EditMarker::kSubstitution;
          cur_entry->edit_distance = achievable_distance;
        }  // end if
      }  // end if lhs_cr and rhs_cr
      rhs_cr->Advance(true);
    }  // end for j
    // Revert back the RHS cursor
    lhs_cr->Advance(true);
    rhs_cr->RetreatEntry(rhs_entry_count);
#ifdef DEBUG
    DCHECK(rhs_cr_cpy == *rhs_cr);
#endif
  }  // end for i

#ifdef DEBUG
  std::ostringstream ss;
  for (size_t i = 0; i <= lhs_entry_count; ++i) {
    for (size_t j = 0; j <= rhs_entry_count; ++j) {
      ss << "[";
      Entry* cur_entry = edit_matrix + i * num_col + j;
      switch (cur_entry->marker) {
        case EditMarker::kNull:
          ss << "oo"; break;
        case EditMarker::kLHSInsertion:
          ss << "+^"; break;
        case EditMarker::kRHSInsertion:
          ss << "+>"; break;
        case EditMarker::kSubstitution:
          ss << "++"; break;
        case EditMarker::kMatch:
          ss << "**"; break;
      }
      ss << ", ";
      ss  << std::setw(4) << cur_entry->edit_distance;
      ss << "] ";
    }  // end for j
    ss << "\n";
  }  // end for i
  DLOG(INFO) << "\n" << ss.str();
#endif

  // Construct index map from edit matrix
  size_t lhs_idx = lhs_entry_count;
  size_t rhs_idx = rhs_entry_count;

  while (lhs_idx > 0 && rhs_idx > 0) {
    Entry* cur_entry = edit_matrix + lhs_idx * num_col + rhs_idx;
    if (cur_entry->marker == EditMarker::kMatch) {
        CHECK_EQ(lhs_entry_elements[lhs_idx - 1],
                 rhs_entry_elements[rhs_idx - 1]);
        IndexRange seq_lhs_range{lhs_range.start_idx
                             + lhs_prefix_acc_elements[lhs_idx - 1],
                             lhs_entry_elements[lhs_idx - 1]};
        IndexRange seq_rhs_range{rhs_range.start_idx
                             + rhs_prefix_acc_elements[rhs_idx - 1],
                             rhs_entry_elements[rhs_idx - 1]};
        result.push_back({seq_lhs_range, seq_rhs_range});
    }  // end if

    switch (cur_entry->marker) {
      case EditMarker::kNull:
        LOG(FATAL) << "Shall not Reach here"; break;
      case EditMarker::kLHSInsertion:
        --lhs_idx; break;
      case EditMarker::kRHSInsertion:
        --rhs_idx; break;
      case EditMarker::kSubstitution:
      case EditMarker::kMatch:
        --lhs_idx; --rhs_idx; break;
    }  // end switch
  }  // end while

  std::reverse(result.begin(), result.end());
  delete[] edit_matrix;
  return IndexRange::Compact(result);
}

uint64_t LevenshteinMapper::numElementsByCursor(const NodeCursor& cursor) {
  CHECK(!cursor.isEnd() && !cursor.isBegin());
  if (!cursor.node()->isLeaf()) {
    MetaEntry me(cursor.current());
    return me.numElements();
  } else {
    return 1;
  }  // end if
}

}  // namespace ustore
