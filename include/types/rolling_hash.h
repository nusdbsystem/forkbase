// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_ROLLING_HASH_H_
#define USTORE_TYPES_ROLLING_HASH_H_

#include <cstdint>
#include <cstddef>
#include "hash/buzhash.h"
#include "types/type.h"

namespace ustore {

class RollingHasher {
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

  RollingHasher(const RollingHasher&) = delete;
  RollingHasher& operator=(const RollingHasher&) = delete;

 private:
  buzhash::BuzHash buz_;
  uint32_t chunk_pattern_;
  size_t window_size_, byte_hashed_ = 0;
  bool crossed_boundary_ = false;
};
}  // namespace ustore

#endif  // USTORE_TYPES_ROLLING_HASH_H_
