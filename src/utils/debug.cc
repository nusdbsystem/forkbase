// Copyright (c) 2017 The Ustore Authors.

#include "utils/debug.h"

#include <cstring>
#include <iomanip>  // std::setfill, std::setw
#include <sstream>

namespace ustore {
std::string byte2str(const byte_t* data, size_t num_bytes) {
  std::ostringstream stm;
  for (size_t i = 0; i < num_bytes; ++i) {
    stm << std::hex << std::setfill('0') << std::setw(2)
        << uint32_t(*(data + i));
  }
  return stm.str();
}

const ustore::byte_t* SpliceBytes(const ustore::byte_t* src,
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

const ustore::byte_t* MultiSplice(
    const ustore::byte_t* original_content,
    size_t original_num_bytes, std::vector<size_t> idxs,
    std::vector<size_t> num_bytes_remove,
    std::vector<const byte_t*> inserted_data,
    std::vector<size_t> inserted_num_bytes,
    size_t* result_num_bytes) {
  size_t total_removed_num_bytes = 0;
  for (const auto i : num_bytes_remove) {
    total_removed_num_bytes += i;
  }

  size_t total_inserted_num_bytes = 0;
  for (const auto i : inserted_num_bytes) {
    total_inserted_num_bytes += i;
  }

  *result_num_bytes = original_num_bytes
                     - total_removed_num_bytes
                     + total_inserted_num_bytes;

  ustore::byte_t* result = new ustore::byte_t[*result_num_bytes];

  size_t offset = 0;
  size_t curr_cpy_idx = 0;

  for (size_t i = 0; i < idxs.size(); ++i) {
    size_t next_cpy_idx = idxs[i];

    // # of bytes from original content to copy to new result
    size_t original_cpy_len = next_cpy_idx - curr_cpy_idx;

    // copy the original content
    std::memcpy(result + offset,
                original_content + curr_cpy_idx,
                original_cpy_len);

    offset += original_cpy_len;

    // copy the appended content
    std::memcpy(result + offset,
                inserted_data[i],
                inserted_num_bytes[i]);

    offset += inserted_num_bytes[i];

    curr_cpy_idx = next_cpy_idx + num_bytes_remove[i];
  }  // end for i

  // deal with the splice overflow the seq end
  if (curr_cpy_idx > original_num_bytes) {
    *result_num_bytes += curr_cpy_idx - original_num_bytes;
    curr_cpy_idx = original_num_bytes;
  }

  std::memcpy(result + offset,
              original_content + curr_cpy_idx,
              original_num_bytes - curr_cpy_idx);

  return result;
}
}  // namespace ustore
