// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_CHUNK_CHUNK_H_
#define USTORE_CHUNK_CHUNK_H_

#include <string>
#include "types/type.h"
#include "hash/hash.h"

namespace ustore {

class Chunk {
 public:
  // allocate a new chunk with usable capacity (excluding meta data)
  Chunk(size_t capacity);
  // share chunk from existing space
  Chunk(byte_t* head);
  ~Chunk();

  // type of the chunk
  inline Type type();
  // total number of bytes
  inline size_t num_bytes();
  // number of bytes used to store actual data
  inline size_t capacity();
  // pointer to the chunk
  inline const byte_t* head() { return head_; }
  // pointer to actual data
  inline const byte_t* data();

 private:
  bool own = false;
  byte_t* head_ = nullptr;
  Hash hash_;
}

}  // namespace ustore
#endif  // USTORE_CHUNK_CHUNK_H_
