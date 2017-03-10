// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_SLICE_H_
#define USTORE_TYPES_SLICE_H_

#include <string>

namespace ustore {

/*
 * slice is a unified type for c and c++ style strings
 * it only pointer to the head of original string, does not copy the content
 */
class Slice {
 public:
  // share data from c++ string
  explicit Slice(const std::string& slice);
  // share data from c string
  explicit Slice(const char* slice);
  ~Slice();

  bool operator<(const Slice& slice) const;
  bool operator>(const Slice& slice) const;
  bool operator==(const Slice& slice) const;

  inline size_t len() { return len_; }
  inline const char* data() { return data_; }

 private:
  size_t len_ = 0;
  char* data_ = nullptr;
}

}  // namespace ustore
#endif  // USTORE_TYPES_SLICE_H_
