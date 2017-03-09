// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_BLOB_H_
#define USTORE_TYPES_BLOB_H_

#include <string>
#include "types/type"

namespace ustore {

/*
 * blob is for a sequence of binray values.
 * it only pointer to the head of original data, does not copy the content
 */
class Blob {
 public:
  // share data from existing array
  explicit Slice(const byte* data, size_t size);
  ~Blob();

  inline size_t size() { return size_; }
  inline const byte* data() { return data_; }

 private:
  size_t size_ = 0;
  byte* data_ = nullptr;
}

}  // namespace ustore
#endif  // USTORE_TYPES_SLICE_H_
