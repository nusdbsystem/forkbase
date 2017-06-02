// Copyright (c) 2017 The Ustore Authors.

#include "types/uiterator.h"

namespace ustore {

bool CursorIterator::next() {
  if (end()) {
    LOG(WARNING) << "Can not move forward as already at seq head. ";
    return false;
  }
  if (ranges_.size() == 0) {
    curr_idx_in_range_ = 0;
    curr_range_idx_ = 0;
    return false;
  }
  if (head()) {
    curr_idx_in_range_ = 0;
    curr_range_idx_ = 0;
    cursor_.Advance(true);
    return true;
  }

  ++curr_idx_in_range_;

  uint64_t curr_range_len = ranges_[curr_range_idx_].num_subsequent;

  if (curr_idx_in_range_ < curr_range_len) {
    cursor_.Advance(true);
  } else if (curr_idx_in_range_ == curr_range_len) {
    // Need to advance to next IndexRange
    curr_idx_in_range_ = 0;
    ++curr_range_idx_;
    auto pre_range = ranges_[curr_range_idx_ - 1];
    if (curr_range_idx_ < ranges_.size()) {
      uint64_t pre_range_end_idx =
          pre_range.start_idx + pre_range.num_subsequent - 1;
      uint64_t steps = ranges_[curr_range_idx_].start_idx
                       - pre_range_end_idx;
      cursor_.AdvanceSteps(steps);
    } else if (curr_range_idx_ == ranges_.size()) {
      // Already at the very end of all index ranges
      // Move cursor one more step
      cursor_.Advance(false);  // either true or false for parameter is fine
    } else {
      LOG(FATAL) << "Invalid curr_range_idx_";
    }
  } else {
    LOG(FATAL) << "Invalid curr_idx_in_range_";
  }

  return !end();
}

bool CursorIterator::previous() {
  if (head()) {
    LOG(WARNING) << "Can not move backward as already at seq head. ";
    return false;
  }
  if (ranges_.size() == 0) {
    curr_range_idx_ = -1;
    curr_idx_in_range_ = 0;
    return false;
  }
  if (end()) {
    curr_range_idx_ = ranges_.size() - 1;
    auto last_range = ranges_[curr_range_idx_];
    curr_idx_in_range_ = last_range.num_subsequent - 1;
    cursor_.Retreat(false);
    return true;
  }

  uint64_t curr_range_len = ranges_[curr_range_idx_].num_subsequent;
  if (curr_idx_in_range_ > 0) {
    --curr_idx_in_range_;
    cursor_.Retreat(true);
  } else if (curr_idx_in_range_ == 0) {
    // Need to retreat to next IndexRange
    --curr_range_idx_;
    if (curr_range_idx_ >= 0) {
      auto next_range = ranges_[curr_range_idx_ + 1];
      auto curr_range_end_idx = ranges_[curr_range_idx_].start_idx
                                + ranges_[curr_range_idx_].num_subsequent - 1;
      uint64_t steps = next_range.start_idx - curr_range_end_idx;
      curr_idx_in_range_ = ranges_[curr_range_idx_].num_subsequent - 1;
      cursor_.RetreatSteps(steps);
    } else if (curr_range_idx_ == -1) {
      // Already at the head of all index ranges
      // Retreat cursor one more step
      cursor_.Retreat(false);  // either true or false for parameter is fine
    } else {
      LOG(FATAL) << "Invalid curr_range_idx_";
    }
  } else {
    LOG(FATAL) << "Invalid curr_idx_in_range_";
  }

  return !head();
}
}  // namespace ustore

