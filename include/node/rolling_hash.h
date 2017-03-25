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
  static const uint32_t DEFAULT_CHUNK_PATTERN =
      (1 << 12) - 1;  // 4KB -- expect boundary pattern
  static const size_t DEFAULT_CHUNK_WINDOW = 64;
  static const size_t DEFAULT_MAXIMUM_CHUNK_SIZE =
      1 << 14;  // 16KB -- hard upper bound of the size of chunks

#ifdef TEST_NODEBUILDER
  // a specific rolling hasher for testing purpose
  // with smaller window size and greater probabiltiy of boundary detection
  inline static RollingHasher* TestHasher() {
    // 256B chunk pattern
    // 1KB maximum chunk size
    // 32B rolling hash window size
    return new RollingHasher(uint32_t((1 << 8) - 1), 32, 1 << 9);
  }
#endif  // TEST_NODEBUILDER

  RollingHasher();
  RollingHasher(uint32_t chunk_pattern, size_t window_size, size_t max_size);

  void HashByte(byte_t b);

  void HashBytes(const byte_t* data, size_t numBytes);

  inline void ClearLastBoundary() {
    crossed_boundary_ = false;
    byte_hashed_ = 0;
  }

  inline void ResetBoundary() { crossed_boundary_ = false; }

  inline bool CrossedBoundary() { return crossed_boundary_; }

  inline size_t window_size() { return window_size_; }

  inline size_t byte_hashed() { return byte_hashed_; }

 private:
  buzhash::BuzHash buz_;
  uint32_t chunk_pattern_;
  size_t window_size_, max_size_, byte_hashed_ = 0;
  bool crossed_boundary_ = false;
};
}  // namespace ustore

#endif  // USTORE_NODE_ROLLING_HASH_H_