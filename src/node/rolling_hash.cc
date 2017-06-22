// Copyright (c) 2017 The Ustore Authors.

#include "node/rolling_hash.h"

namespace ustore {

RollingHasher::RollingHasher()
    : RollingHasher(kDefaultChunkPattern, kDefaultChunkWindow,
                    kDefaultMaxChunkSize) {}

RollingHasher::RollingHasher(uint32_t chunk_pattern, size_t window_size,
                             size_t max_size)
    : chunk_pattern_{chunk_pattern},
      window_size_{window_size},
      max_size_{max_size},
      buz_{unsigned(window_size)} {}

void RollingHasher::HashBytes(const byte_t* data, size_t numBytes) {
  for (size_t i = 0; i < numBytes; i++) {
    HashByte(*(data + i));
  }
}

size_t RollingHasher::TryHashBytes(const byte_t* data, size_t numBytes) {
  for (size_t i = 0; i < numBytes; i++) {
    HashByte(*(data + i));
    if (crossed_boundary_) return i;
  }
  return numBytes;
}

}  // namespace ustore
