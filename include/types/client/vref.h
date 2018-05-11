// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VREF_H_
#define USTORE_TYPES_CLIENT_VREF_H_

#include <memory>
#include <utility>
#include "hash/hash.h"
#include "types/type.h"

namespace ustore {

class VRef : public VInit {
 public:
  VRef() = default;
  VRef(DB* db, Slice ptt_key, UType type, Hash hash)
    : VInit(db), ptt_key_(ptt_key), type_(type), hash_(hash) {}
  VRef(DB* db, Slice ptt_key, UType type, Hash hash,
       std::shared_ptr<ChunkLoader> loader)
    : VInit(db, loader), ptt_key_(ptt_key), type_(type), hash_(hash) {}
  ~VRef() = default;

  UType type() const override { return type_; }
  Hash dataHash() const override { return hash_; }
  Slice partitionKey() const override { return ptt_key_; }

  friend std::ostream& operator<<(std::ostream& os, const VRef& obj) {
    os << static_cast<const VInit&>(obj);
    return os;
  }

 private:
  Slice ptt_key_;
  UType type_;
  Hash hash_;
};

}  // namespace ustore

#endif  //  USTORE_TYPES_CLIENT_VREF_H_
