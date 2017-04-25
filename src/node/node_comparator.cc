// Copyright (c) 2017 The Ustore Authors.

#include "node/node_comparator.h"

namespace ustore {
std::vector<IndexRange> CompactRanges(
    const std::vector<IndexRange>& ranges) {
  if (ranges.size() == 0) return ranges;

  std::vector<IndexRange> result;

  IndexRange curr_cr = ranges[0];
  uint64_t pre_upper = curr_cr.start_idx + curr_cr.num_subsequent;

  for (size_t i = 1; i < ranges.size(); ++i) {
    CHECK_NE(ranges[i].num_subsequent, 0);
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
}  // namespace ustore
