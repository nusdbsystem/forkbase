// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_USTRING_H_
#define USTORE_TYPES_USTRING_H_

#include <cstddef>
#include "chunk/chunk.h"
#include "hash/hash.h"
#include "node/string_node.h"
#include "types/type.h"
#include "utils/noncopyable.h"

namespace ustore {

class UString : private Noncopyable {
 public:
  // create the UString based on the data
  //   dump the created chunk into storage
  static const UString* Create(const byte_t* data, size_t num_bytes);
  static const UString* Load(const Hash& hash);

  ~UString() { delete node_; }

  inline size_t len() const { return node_->len();}
  // copy string contents to buffer
  //   return string length
  // TODO(pingcheng): only need to provide non-copy read api
  inline const size_t data(byte_t* buffer) const { return node_->Copy(buffer); }
  // hash of this ucell
  inline const Hash hash() const { return node_->hash(); }

 private:
  // Private construcstor to create an instance based on the root chunk data
  // To be called by Load() and Init()
  explicit UString(const Chunk* chunk);

  // Responsible to remove during destructing
  const StringNode* node_;
};

}  // namespace ustore
#endif  // USTORE_TYPES_USTRING_H_
