// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILS_DEBUG_H_
#define USTORE_UTILS_DEBUG_H_

#include <cstring>
#include <iomanip>  // std::setfill, std::setw
#include <sstream>
#include <string>
#include "types/type.h"

namespace ustore {

static inline std::string byte2str(const byte_t* data, size_t num_bytes) {
  std::ostringstream stm;
  for (size_t i = 0; i < num_bytes; ++i) {
    stm << std::hex << std::setfill('0') << std::setw(2)
        << uint32_t(*(data + i));
  }
  return stm.str();
}

// A utility method to splice bytes array, delete @arg num_delete bytes,
// from @arg start, then insert @arg append_size number of bytes from
// input buffer @arg append. This method will not rewrite the original
// @arg src but copy the content of it. Caller of this method is responsible
// for delete of the created buffer.
static inline const ustore::byte_t* SpliceBytes(const ustore::byte_t* src,
    size_t src_size, size_t start, size_t num_delete,
    const ustore::byte_t* append, size_t append_size) {
  if (start > src_size) start = src_size;
  size_t real_delete = num_delete;
  if (src_size - start < num_delete) real_delete = src_size - start;
  byte_t* result = new byte_t[src_size - real_delete + append_size];
  std::memcpy(result, src, start);
  std::memcpy(result + start, append, append_size);
  std::memcpy(result + start + append_size, src + start + real_delete,
              src_size - start - real_delete);
  return result;
}

}  // namespace ustore
#endif  // USTORE_UTILS_DEBUG_H_
