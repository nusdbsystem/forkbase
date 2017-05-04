// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_UITERATOR_H_
#define USTORE_TYPES_UITERATOR_H_

#include <memory>
#include <type_traits>
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


template <class iterator_trait>
class DuallyDiffIterator {
  // a special iterator that iterate on the dual diff
 public:
  //  The keys/index must be in strictly increasing order during iteration
  //  two iterator must be both at head
  DuallyDiffIterator(std::unique_ptr<UIterator> lhs_diff_it,
                     std::unique_ptr<UIterator> rhs_diff_it) noexcept :
      lhs_diff_it_(std::move(lhs_diff_it)),
      rhs_diff_it_(std::move(rhs_diff_it)),
      just_advanced(true) {
        CHECK(head());
        lhs_diff_it_->next();
        rhs_diff_it_->next();
        update_flag(false);
  }

  // return lhs value for the current key, which is different
  //   from rhs
  // return empty slice if the current key does not
  // exist in lhs
  Slice lhs_value() const;

  // return rhs value for the current key, which is different
  //   from lhs
  // return empty slice if the current key does not
  // exist in rhs
  Slice rhs_value() const;

  inline Slice key() const {
    return t_key(std::integral_constant<bool, iterator_trait::key_supported>());
  }

  inline uint64_t index() const {
    return t_index(std::integral_constant<bool,
                       iterator_trait::index_supported>());
  }

  bool next();

  bool previous();

  inline bool end() const {
    return lhs_diff_it_->end() && rhs_diff_it_->end();
  }

  inline bool head() const {
    return lhs_diff_it_->head() && rhs_diff_it_->head();
  }

 private:
  // update it_flag_ based on lhs and rhs key comparison
  //   return the updated flag
  int8_t update_flag(bool isGreaterValid);

  inline Slice t_key(std::true_type) const {
    if (it_flag_ <= 0) {
      return lhs_diff_it_->key();
    } else {
      return rhs_diff_it_->key();
    }
  }

  inline Slice t_key(std::false_type) const {
    LOG(FATAL) << "Not supported for key.";
    return Slice(nullptr, 0);
  }

  inline uint64_t t_index(std::true_type) const {
    if (it_flag_ <= 0) {
      return lhs_diff_it_->index();
    } else {
      return rhs_diff_it_->index();
    }
  }

  inline uint64_t t_index(std::false_type) const {
    LOG(FATAL) << "Not supported for Index";
    return 0;
  }

  std::unique_ptr<UIterator> lhs_diff_it_;
  std::unique_ptr<UIterator> rhs_diff_it_;

  // -1 to indicate lhs_diff is valid
  // 0 to indicate both valid
  // 1 to indicate rhs_diff is valid
  int8_t it_flag_;

  // true if the previous operation is next
  // false if the previous operation is previous
  bool just_advanced;
};


template <class iterator_trait>
Slice DuallyDiffIterator<iterator_trait>::lhs_value() const {
  if (it_flag_ <= 0) {
    return lhs_diff_it_->value();
  } else {
    // return empty slice
    return Slice(nullptr, 0);
  }
}

template <class iterator_trait>
Slice DuallyDiffIterator<iterator_trait>::rhs_value() const {
  if (it_flag_ >= 0) {
    return rhs_diff_it_->value();
  } else {
    // return empty slice
    return Slice(nullptr, 0);
  }
}

template <class iterator_trait>
bool DuallyDiffIterator<iterator_trait>::next() {
  // if (it_flag_ == -1) {
  //   DLOG(INFO) << "Next For Flag: -1";
  // } else if (it_flag_ == 0) {
  //   DLOG(INFO) << "Next For Flag: 0";
  // } else {
  //   DLOG(INFO) << "Next For Flag: 1";
  // }
  // DLOG(INFO) << "Key Len: " << key().len();

  if (!just_advanced) {
    just_advanced = true;
    // DLOG(INFO) << "Just previous()";
    // previously perform a previous() operation
    if (it_flag_ == -1) {
      DCHECK(rhs_diff_it_->head() ||
             iterator_trait::key(*lhs_diff_it_) >
             iterator_trait::key(*rhs_diff_it_));

      rhs_diff_it_->next();

      DCHECK(rhs_diff_it_->end() ||
             iterator_trait::key(*lhs_diff_it_) <
             iterator_trait::key(*rhs_diff_it_));
    } else if (it_flag_ == 1) {
      DCHECK(lhs_diff_it_->head() ||
             iterator_trait::key(*rhs_diff_it_) >
             iterator_trait::key(*lhs_diff_it_));

      lhs_diff_it_->next();

      DCHECK(lhs_diff_it_->end() ||
             iterator_trait::key(*rhs_diff_it_) <
             iterator_trait::key(*lhs_diff_it_));
    }
  }  // end just_advanced

  if (end()) {
    // do nothing
  } else if (head()) {
    lhs_diff_it_->next();
    rhs_diff_it_->next();
  } else if (it_flag_ == -1) {
    lhs_diff_it_->next();
  } else if (it_flag_ == 0) {
    lhs_diff_it_->next();
    rhs_diff_it_->next();
  } else if (it_flag_ == 1) {
    rhs_diff_it_->next();
  } else {
    LOG(FATAL) << "No other choice.";
  }

  // iterator with smaller current key is the valid one
  update_flag(false);
  return !end();
}

template <class iterator_trait>
bool DuallyDiffIterator<iterator_trait>::previous() {
  // if (it_flag_ == -1) {
  //   DLOG(INFO) << "Previous For Flag: -1";
  // } else if (it_flag_ == 0) {
  //   DLOG(INFO) << "Previous For Flag: 0";
  // } else {
  //   DLOG(INFO) << "Previous For Flag: 1";
  // }
  // DLOG(INFO) << "Key Len: " << key().len();

  if (just_advanced) {
    // DLOG(INFO) << "Just next()";
    just_advanced = false;

    // previously perform a next() operation
    if (it_flag_ == -1) {
      DCHECK(rhs_diff_it_->end() ||
             iterator_trait::key(*lhs_diff_it_) <
             iterator_trait::key(*rhs_diff_it_));

      rhs_diff_it_->previous();

      DCHECK(rhs_diff_it_->head() ||
             iterator_trait::key(*lhs_diff_it_) >
             iterator_trait::key(*rhs_diff_it_));

    } else if (it_flag_ == 1) {
      DCHECK(lhs_diff_it_->end() ||
             iterator_trait::key(*rhs_diff_it_) <
             iterator_trait::key(*lhs_diff_it_));

      lhs_diff_it_->previous();

      DCHECK(lhs_diff_it_->head() ||
             iterator_trait::key(*rhs_diff_it_) >
             iterator_trait::key(*lhs_diff_it_));
    }
  }  // end just_advanced


  // previous perform a previous() operation
  if (head()) {
    // do nothing
  } else if (end()) {
    lhs_diff_it_->previous();
    rhs_diff_it_->previous();
  } else if (it_flag_ == -1) {
    lhs_diff_it_->previous();
  } else if (it_flag_ == 0) {
    lhs_diff_it_->previous();
    rhs_diff_it_->previous();
  } else if (it_flag_ == 1) {
    rhs_diff_it_->previous();
  } else {
    LOG(FATAL) << "No other choice.";
  }

  // iterator with greater current key is the valid one
  update_flag(true);
  return !head();
}

template <class iterator_trait>
int8_t DuallyDiffIterator<iterator_trait>::update_flag(bool isGreaterValid) {
  // DLOG(INFO) << "Updating Flag.";
  if (head() || end()) {
    // DLOG(INFO) << "head() or end()";
    it_flag_ = 0;
  } else if (lhs_diff_it_->head() || lhs_diff_it_->end()) {
    // DLOG(INFO) << "lhs head() or end()";
    it_flag_ = 1;  // rhs is valid
  } else if (rhs_diff_it_->head() || rhs_diff_it_->end()) {
    // DLOG(INFO) << "rhs head() or end()";
    it_flag_ = -1;  // lhs is valid
  } else if (iterator_trait::key(*lhs_diff_it_) <
             iterator_trait::key(*rhs_diff_it_)) {
    it_flag_ = isGreaterValid? 1 : -1;
  } else if (iterator_trait::key(*lhs_diff_it_) ==
             iterator_trait::key(*rhs_diff_it_)) {
    it_flag_ = 0;
  } else if (iterator_trait::key(*lhs_diff_it_) >
             iterator_trait::key(*rhs_diff_it_)) {
    it_flag_ = isGreaterValid? -1 : 1;
  }
  // if (it_flag_ == -1) {
  //   DLOG(INFO) << "Updated Flag: -1";
  // } else if (it_flag_ == 0) {
  //   DLOG(INFO) << "Updated Flag: 0";
  // } else {
  //   DLOG(INFO) << "Updated Flag: 1";
  // }
  return it_flag_;
}


struct iterator_index_trait {
  static uint64_t key(const UIterator& it) {
    return it.index();
  }

  static constexpr bool index_supported = true;

  static constexpr bool key_supported = false;
};

struct iterator_key_trait {
  static Slice key(const UIterator& it) {
    return it.key();
  }

  static constexpr bool index_supported = false;

  static constexpr bool key_supported = true;
};

using DuallyDiffIndexIterator = DuallyDiffIterator<iterator_index_trait>;
using DuallyDiffKeyIterator = DuallyDiffIterator<iterator_key_trait>;
}  // namespace ustore
#endif  // USTORE_TYPES_UITERATOR_H_