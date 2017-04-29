// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_UITERATOR_H_
#define USTORE_TYPES_UITERATOR_H_

#include <memory>
#include <utility>
#include <vector>

#include "node/cursor.h"
#include "spec/slice.h"
#include "utils/noncopyable.h"

namespace ustore {

class UIterator : private Noncopyable {
 public:
  UIterator(const Hash& root,
            const std::vector<IndexRange>& ranges,
            ChunkLoader* loader) noexcept :
      ranges_(ranges), curr_range_idx_(0),
      curr_idx_in_range_(0) {
    CHECK_LT(0, ranges_.size());
    cursor_ = std::unique_ptr<NodeCursor>(
                  NodeCursor::GetCursorByIndex(root, index(), loader));
  }

  UIterator(const Hash& root,
            std::vector<IndexRange>&& ranges,
            ChunkLoader* loader) noexcept :
      ranges_(std::move(ranges)), curr_range_idx_(0),
      curr_idx_in_range_(0) {
    CHECK_LT(0, ranges_.size());
    cursor_ = std::unique_ptr<NodeCursor>(
                  NodeCursor::GetCursorByIndex(root, index(), loader));
  }

  // point to next element
  //  return false if cursor points to end after movement
  bool next();

  // point to previous element
  //  return false if cursor points to head after movement
  bool previous();

  inline bool head() const { return curr_range_idx_ == -1; }

  inline bool end() const {return curr_range_idx_ == ranges_.size(); }

  // return the idx of pointed element
  virtual inline uint64_t index() const {
    CHECK(!head() && !end());
    return ranges_[curr_range_idx_].start_idx + curr_idx_in_range_;
  }

  // return the decoded slice value
  virtual inline Slice key() const {
    CHECK(!head() && !end());
    return cursor_->currentKey().ToSlice();
  }

  // return the decoded slice value
  inline Slice value() const {
    CHECK(!head() && !end());
    return RealValue();
  }

  // Override this method to return d
  virtual Slice RealValue() const {
    return Slice(reinterpret_cast<const char*>(data()),
                 numBytes());
  }

 protected:
  inline const byte_t* data() const {
    return cursor_->current();
  }

  inline size_t numBytes() const {
    return cursor_->numCurrentBytes();
  }

 private:

  std::vector<IndexRange> ranges_;

  // the index of current IndexRange in ranges_ pointed by iterator
  // -1 if iterator at head
  // ranges_.size() if iterator at end
  int32_t curr_range_idx_;

  // the index of element in current IndexRange pointed by iterator
  uint64_t curr_idx_in_range_;

  std::unique_ptr<NodeCursor> cursor_;
};
}  // namespace ustore
#endif  // USTORE_TYPES_UITERATOR_H_
