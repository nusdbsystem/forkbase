// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_USTRING_H_
#define USTORE_TYPES_USTRING_H_

#include <cstddef>

#include "chunk/chunk.h"
#include "hash/hash.h"
#include "types/type.h"
#include "utils/noncopyable.h"
#include "node/string_node.h"

namespace ustore {

class UString : private Noncopyable {
 public:
  static UString Load(const Hash& hash);
  static UString Create(byte_t* data, size_t num_bytes);

  inline size_t len() const { return node_->len();}

 private:
  // Private construcstor to create an instance based on the root chunk data
  // To be called by Load() and Init()
  explicit UString(const Chunk* chunk);

  // Responsible to remove during destructing
  const StringNode* node_;
};

}  // namespace ustore
#endif  // USTORE_TYPES_USTRING_H_
