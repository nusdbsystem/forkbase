// Copyright (c) 2017 The Ustore Authors.

#include "node/rolling_hash.h"

namespace ustore {

RollingHasher::RollingHasher()
    : chunk_pattern_{DEFAULT_CHUNK_PATTERN},
      window_size_{DEFAULT_CHUNK_WINDOW},
      max_size_{DEFAULT_MAXIMUM_CHUNK_SIZE},
      buz_{unsigned(DEFAULT_CHUNK_WINDOW)} {}

RollingHasher::RollingHasher(uint32_t chunk_pattern, size_t window_size,
                             size_t max_size)
    : chunk_pattern_{chunk_pattern},
      window_size_{window_size},
      max_size_{max_size},
      buz_{unsigned(window_size)} {}

void RollingHasher::HashByte(byte_t b) {
  ++byte_hashed_;
  buz_.HashByte(b);
  crossed_boundary_ = (byte_hashed_ >= window_size_) &&
                      (crossed_boundary_ ||
                       ((buz_.Sum32() & chunk_pattern_) == chunk_pattern_) ||
                       (byte_hashed_ == max_size_));
}

void RollingHasher::HashBytes(const byte_t* data, size_t numBytes) {
  for (size_t i = 0; i < numBytes; i++) {
    HashByte(*(data + i));
  }
}
}  // namespace ustore
