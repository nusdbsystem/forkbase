// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_ROLLING_HASH_H_
#define USTORE_NODE_ROLLING_HASH_H_

#include <cstddef>
#include <cstdint>
#include "hash/buzhash.h"
#include "types/type.h"
#include "utils/noncopyable.h"

namespace ustore {

class RollingHasher : private Noncopyable {
 public:
  static const uint32_t DEFAULT_CHUNK_PATTERN = (1 << 12) - 1;  // 4KB
  static const size_t DEFAULT_CHUNK_WINDOW = 64;

  RollingHasher();
  RollingHasher(uint32_t chunk_pattern, size_t window_size);

  void HashByte(byte_t b);
  inline void ClearLastBoundary() {
    crossed_boundary_ = false;
    byte_hashed_ = 0;
  }
  inline bool CrossedBoundary() { return crossed_boundary_; }

 private:
  buzhash::BuzHash buz_;
  uint32_t chunk_pattern_;
  size_t window_size_, byte_hashed_ = 0;
  bool crossed_boundary_ = false;
};
}  // namespace ustore

#endif  // USTORE_NODE_ROLLING_HASH_H_
