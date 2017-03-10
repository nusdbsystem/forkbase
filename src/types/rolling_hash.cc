#include "types/rolling_hash.h"

namespace ustore {

RollingHasher::RollingHasher()
    : chunk_pattern_{DEFAULT_CHUNK_PATTERN},
      window_size_{DEFAULT_CHUNK_WINDOW},
      buz_{unsigned(DEFAULT_CHUNK_WINDOW)} {}

RollingHasher::RollingHasher(uint32_t chunk_pattern, size_t window_size)
    : chunk_pattern_{chunk_pattern},
      window_size_{window_size},
      buz_{unsigned(window_size_)} {}

void RollingHasher::HashByte(byte_t b) {
  ++byte_hashed_;
  buz_.HashByte(b);
  crossed_boundary_ =
      crossed_boundary_ || ((buz_.Sum32() & chunk_pattern_) == chunk_pattern_);
}

}  // namespace ustore
