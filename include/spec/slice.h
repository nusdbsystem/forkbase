// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_SPEC_SLICE_H_
#define USTORE_SPEC_SLICE_H_

#include <algorithm>
#include <cstring>
#include <iostream>
#include <string>
#include <utility>
#include "hash/murmurhash.h"

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
  explicit Slice(std::string&& slice) = delete;
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

  friend inline std::ostream& operator<<(std::ostream& os, const Slice& obj) {
    os << std::string(obj.data_, obj.len_);
    return os;
  }

 private:
  const char* data_ = nullptr;
  size_t len_ = 0;
};

/* Slice variant to ensure always point to valid string when used in containers.
 * Copy a string when stored in containers, not copy when lookup.
 */
class PSlice {
 public:
  // Persist a slice
  static PSlice Persist(const Slice& slice) {
    PSlice ps(slice);
    ps.value_ = std::string(slice.data(), slice.len());
    ps.slice_ = Slice(ps.value_);
    return ps;
  }

  // Do not persist a slice
  PSlice(const Slice& slice) : slice_(slice) {}  // NOLINT
  PSlice(const PSlice& pslice)
    : value_(pslice.value_), slice_(pslice.slice_) {
    if (!value_.empty()) slice_ = Slice(value_);
  }
  PSlice(PSlice&& pslice)
    : value_(std::move(pslice.value_)), slice_(pslice.slice_) {
    if (!value_.empty()) slice_ = Slice(value_);
  }

  inline PSlice& operator=(PSlice pslice) {
    std::swap(value_, pslice.value_);
    slice_ = value_.empty() ? pslice.slice_ : Slice(value_);
    return *this;
  }

  inline bool operator==(const PSlice& pslice) const {
    return slice_ == pslice.slice_;
  }
  inline const Slice& slice() const { return slice_; }

 private:
  Slice slice_;
  std::string value_;
};

}  // namespace ustore

namespace std {

template<>
struct hash<::ustore::Slice> {
  inline size_t operator()(const ::ustore::Slice& obj) const {
    return ::ustore::MurmurHash(obj.data(), obj.len());
  }
};

template<>
struct hash<::ustore::PSlice> {
  inline size_t operator()(const ::ustore::PSlice& obj) const {
    return hash<::ustore::Slice>()(obj.slice());
  }
};

}  // namespace std

#endif  // USTORE_SPEC_SLICE_H_
