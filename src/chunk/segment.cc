// Copyright (c) 2017 The Ustore Authors.

#include "chunk/segment.h"

#include <algorithm>
#include <cstring>  // for memcpy
#include <utility>  // for std::move()
#include "utils/logging.h"

namespace ustore {
void Segment::AppendForChunk(byte_t* chunk_buffer) const {
  std::memcpy(chunk_buffer, data_, num_bytes_);
}

const byte_t* FixedSegment::entry(size_t idx) const {
  CHECK_LE(0, idx);
  CHECK_GT(numEntries(), idx);
  return data_ + idx * bytes_per_entry_;
}

size_t FixedSegment::prolong(size_t entry_num_bytes) {
  CHECK_EQ(entry_num_bytes % bytes_per_entry_, 0);
  num_bytes_ += entry_num_bytes;
  return numEntries();
}

size_t FixedSegment::entryNumBytes(size_t idx) const {
  CHECK_LE(0, idx);
  CHECK_GT(numEntries(), idx);
  return bytes_per_entry_;
}

size_t FixedSegment::PosToIdx(size_t pos) const {
  CHECK_LE(0, pos);
  CHECK_GE(numBytes(), pos);
  return pos / bytes_per_entry_;
}

std::pair<std::unique_ptr<const Segment>, std::unique_ptr<const Segment>>
FixedSegment::Split(size_t idx) const {
  CHECK(!empty());
  CHECK_LE(0, idx);
  CHECK_GE(numEntries(), idx);
  size_t preSegBytes = bytes_per_entry_ * idx;
  size_t postSegBytes = numBytes() - preSegBytes;
  const byte_t* preData = entry(0);
  const byte_t* postData = nullptr;
  if (idx < numEntries()) {
    postData = entry(idx);
  }
  std::pair<std::unique_ptr<const Segment>, std::unique_ptr<const Segment>>
      split_segs = {std::unique_ptr<const Segment>(new FixedSegment(
                        preData, preSegBytes, bytes_per_entry_)),
                    std::unique_ptr<const Segment>(new FixedSegment(
                        postData, postSegBytes, bytes_per_entry_))};
  return split_segs;
}

const byte_t* VarSegment::entry(size_t idx) const {
  CHECK_LE(0, idx);
  CHECK_GT(numEntries(), idx);
  return data_ + entry_offsets_[idx];
}

size_t VarSegment::prolong(size_t entry_num_bytes) {
  entry_offsets_.push_back(num_bytes_);
  num_bytes_ += entry_num_bytes;
  return numEntries();
}

inline size_t VarSegment::entryNumBytes(size_t idx) const {
  CHECK_LE(0, idx);
  CHECK_GT(numEntries(), idx);
  return (idx == numEntries() - 1 ? numBytes() : entry_offsets_[idx + 1]) -
         entry_offsets_[idx];
}

size_t VarSegment::PosToIdx(size_t pos) const {
  CHECK_LE(0, pos);
  CHECK_GE(numBytes(), pos);
  return std::upper_bound(entry_offsets_.begin(), entry_offsets_.end(), pos) -
         entry_offsets_.begin() - 1;
}

std::pair<std::unique_ptr<const Segment>, std::unique_ptr<const Segment>>
VarSegment::Split(size_t idx) const {
  CHECK(!empty());
  CHECK_LE(0, idx);
  CHECK_GE(numEntries(), idx);
  size_t preSegBytes = 0;
  const byte_t* preData = entry(0);
  const byte_t* postData = nullptr;
  if (idx < numEntries()) {
    preSegBytes = entry_offsets_[idx];
    postData = entry(idx);
  } else {
    preSegBytes = numBytes();
  }
  size_t postSegBytes = numBytes() - preSegBytes;
  std::vector<size_t> preOffsets(entry_offsets_.begin(),
                                 entry_offsets_.begin() + idx);
  std::vector<size_t> postOffsets;
  for (size_t i = idx; i < numEntries(); i++) {
    postOffsets.push_back(entry_offsets_[i] - entry_offsets_[idx]);
  }
  std::pair<std::unique_ptr<const Segment>, std::unique_ptr<const Segment>>
      split_segs = {std::unique_ptr<const Segment>(new VarSegment(
                        preData, preSegBytes, std::move(preOffsets))),
                    std::unique_ptr<const Segment>(new VarSegment(
                        postData, postSegBytes, std::move(postOffsets)))};
  return split_segs;
}

}  // namespace ustore
