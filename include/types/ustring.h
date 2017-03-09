// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_USTRING_H_
#define USTORE_TYPES_USTRING_H_

#include <cstddef>

#include "types/type.h"
#include "hash/hash.h"
#include "chunk/chunk.h"

namespace ustore {
class UString {
 public:
  static UString Load(const Hash& hash);

  static UString Create(byte* data, size_t num_bytes);

  inline size_t len() const {node_->len();}

 private:
  // hash of node_
  // make sure they in sync
  const Hash node_hash_;

  // Responsible to remove during destructing
  const StringNode* node_;

  // Private construcstor to create an instance based on the root chunk data
  // To be called by Load() and Init()
  explicit UString(const Chunk* chunk);

  UString(const UString&) = delete  // disable copy-constructor
  UString& operator=(const UString&) = delete;  // disable copy-assignment
};

}  // namespace ustore
#endif  // USTORE_TYPES_USTRING_H_
