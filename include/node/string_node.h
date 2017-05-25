// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_STRING_NODE_H_
#define USTORE_NODE_STRING_NODE_H_

#include "chunk/chunk.h"
#include "types/base.h"

namespace ustore {
class StringNode : public UNode {
/* StringNode contains a single string.

  Encoding Scheme:
  | -str_len- | ------string bytes ------|
  | --------- 4 ------variable size

*/
 public:
  static Chunk NewChunk(const byte_t* data, size_t num_bytes);

  explicit StringNode(const Chunk* chunk) : UNode(chunk) {}
  ~StringNode() = default;

  size_t len() const;  // the byte count of this string
// Copy all the string bytes to buffer
// Buffer capacity shall be larger than this string len
// return the number of bytes copied.
  size_t Copy(byte_t* buffer) const;

  // return the pointer to the string first byte
  //   nullptr if len is 0
  inline const byte_t* Read() const {
    return len() ? chunk_->data() + sizeof(uint32_t)
                 : nullptr;
  }
};
}  // namespace ustore

#endif  // USTORE_NODE_STRING_NODE_H_
