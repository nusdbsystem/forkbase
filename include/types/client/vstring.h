// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VSTRING_H_
#define USTORE_TYPES_CLIENT_VSTRING_H_

#include <memory>
#include "types/client/vvalue.h"
#include "types/ustring.h"

namespace ustore {

class VString : public UString, public VValue {
 public:
  ~VString() = default;

 protected:
  // Load an existing VString
  VString(std::shared_ptr<ChunkLoader>, const Hash& hash) noexcept;
  // Creata a new VString
  VString(std::shared_ptr<ChunkLoader>, const Slice& data) noexcept;
};

}  // namespace ustore

#endif  // USTORE_TYPES_CLIENT_VSTRING_H_
