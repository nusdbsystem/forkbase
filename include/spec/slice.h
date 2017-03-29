// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_SPEC_SLICE_H_
#define USTORE_SPEC_SLICE_H_

#include <algorithm>
#include <cstring>
#include <string>

namespace ustore {

/*
 * slice is a unified type for c and c++ style strings
 * it only pointer to the head of original string, does not copy the content
 */
class Slice {
 public:
  // share data from c++ string
  explicit Slice(const std::string& slice) {
    len_ = slice.length();
    data_ = slice.data();
  }
  // share data from c string
  explicit Slice(const char* slice) {
    len_ = std::strlen(slice);
    data_ = slice;
  }
  ~Slice() {}

  inline bool operator<(const Slice& slice) const {
    size_t min_len = std::min(len_, slice.len_);
    int cmp = std::memcmp(data_, slice.data_, min_len);
    return cmp ? cmp < 0 : len_ < slice.len_;
  }
  inline bool operator>(const Slice& slice) const {
    size_t min_len = std::min(len_, slice.len_);
    int cmp = std::memcmp(data_, slice.data_, min_len);
    return cmp ? cmp > 0 : len_ > slice.len_;
  }
  inline bool operator==(const Slice& slice) const {
    if (len_ != slice.len_) return false;
    return std::memcmp(data_, slice.data_, len_) == 0;
  }

  inline size_t len() const { return len_; }
  inline const char* data() const { return data_; }

 private:
  size_t len_ = 0;
  const char* data_ = nullptr;
};
}  // namespace ustore

#endif  // USTORE_SPEC_SLICE_H_
