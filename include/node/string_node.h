// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_STRING_NODE_H_
#define USTORE_NODE_STRING_NODE_H_

#include <cstddef>
#include <memory>
#include "chunk/chunk.h"
#include "types/type.h"

namespace ustore {
class StringNode {
/* StringNode contains a single string.

  Encoding Scheme:
  | -str_len- | ------string bytes ------|
  | --------- 4 ------variable size

*/
 public:
  static const Chunk* NewChunk(const byte_t* data, size_t num_bytes);

  explicit StringNode(const Chunk* chunk) : chunk_(chunk) {}
  ~StringNode() {}

  size_t len() const;  // the byte count of this string
// Copy all the string bytes to buffer
// Buffer capacity shall be larger than this string len
// return the number of bytes copied.
  size_t Copy(byte_t* buffer) const;

  // hash of this node
  inline const Hash hash() const { return chunk_->hash(); }

 private:
  std::unique_ptr<const Chunk> chunk_;
};
}  // namespace ustore

#endif  // USTORE_NODE_STRING_NODE_H_
