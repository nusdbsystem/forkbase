// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_SPEC_BLOB_H_
#define USTORE_SPEC_BLOB_H_

#include <cstring>
#include <string>
#include "types/type.h"

namespace ustore {

/*
 * blob is for a sequence of binray values.
 * it only pointer to the head of original data, does not copy the content
 */
class Blob {
 public:
  // share data from existing array
  Blob(const Blob& blob) : data_(blob.data_), size_(blob.size_) {}
  Blob(const byte_t* data, size_t size) : data_(data), size_(size) {}
  ~Blob() {}

  inline Blob& operator=(const Blob& blob) {
    data_ = blob.data_;
    size_ = blob.size_;
    return *this;
  }

  inline bool operator==(const Blob& blob) const {
    if (size_ != blob.size_) return false;
    return std::memcmp(data_, blob.data_, size_) == 0;
  }

  inline size_t size() const { return size_; }
  inline const byte_t* data() const { return data_; }

 private:
  const byte_t* data_ = nullptr;
  size_t size_ = 0;
};
}  // namespace ustore

#endif  // USTORE_SPEC_BLOB_H_
