// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_SPEC_SLICE_H_
#define USTORE_SPEC_SLICE_H_

#include <algorithm>
#include <cstring>
#include <iostream>
#include <string>
#include "hash/murmurhash_ustore.hpp"

namespace ustore {

/**
 * Slice is a unified type for C and C++ style strings.
 * It only points to the head of original string, does not copy the content.
 */
class Slice {
 public:
  Slice(const Slice& slice) : data_(slice.data_), len_(slice.len_) {}
  // share data from c++ string
  explicit Slice(const std::string& slice)
    : data_(slice.data()), len_(slice.length()) {}
  // delete constructor that takes in rvalue std::string
  //   to avoid the memory space of parameter is released unawares.
  Slice(std::string&& slice) = delete;
  // share data from c string
  explicit Slice(const char* slice) : data_(slice) {
    len_ = std::strlen(slice);
  }
  Slice(const char* slice, size_t len) : data_(slice), len_(len) {}
  ~Slice() {}

  inline Slice& operator=(const Slice& slice) {
    data_ = slice.data_;
    len_ = slice.len_;
    return *this;
  }

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

  inline bool empty() const { return len_ == 0; }
  inline size_t len() const { return len_; }
  inline const char* data() const { return data_; }

  friend std::ostream& operator<<(std::ostream &, const Slice &);

 private:
  const char* data_ = nullptr;
  size_t len_ = 0;
};

inline std::ostream& operator<<(std::ostream& os, const Slice & obj) {
  os << std::string(obj.data(), obj.len());
  return os;
}

}  // namespace ustore

namespace std {
template<>
struct hash<ustore::Slice> {
  inline size_t operator()(const ustore::Slice & obj) const {
    return ustore::MurmurHash(obj.data(), obj.len());
  }
};
} // namespace std

#endif  // USTORE_SPEC_SLICE_H_
