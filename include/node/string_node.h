// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_NODE_STRING_NODE_H_
#define USTORE_NODE_STRING_NODE_H_

#include <cstddef>
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
  explicit StringNode(const Chunk* chunk);
  ~StringNode();

  inline size_t len() const;  // the byte count of this string

// Copy all the string bytes to buffer
// Buffer capacity shall be larger than this string len
// return the number of bytes copied.
  size_t Copy(byte_t* buffer) const;

 private:
  const Chunk* chunk_;
};
}  // namespace ustore

#endif  // USTORE_NODE_STRING_NODE_H_
