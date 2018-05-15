// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VREF_H_
#define USTORE_TYPES_CLIENT_VREF_H_

#include <memory>
#include <utility>
#include "hash/hash.h"
#include "spec/slice.h"
#include "types/type.h"
#include "types/client/vhandler.h"

namespace ustore {

class VRef : public VHandler {
 public:
  VRef() = default;
  VRef(DB* db, Slice route_key, UType type, Hash hash)
    : VHandler(db), route_key_(route_key), type_(type), hash_(hash) {}
  VRef(DB* db, Slice route_key, UType type, Hash hash,
       std::shared_ptr<ChunkLoader> loader)
    : VHandler(db, loader), route_key_(route_key), type_(type), hash_(hash) {}
  ~VRef() = default;

  UType type() const override { return type_; }
  Hash dataHash() const override { return hash_; }
  Slice partitionKey() const override { return route_key_; }

  friend std::ostream& operator<<(std::ostream& os, const VRef& obj) {
    os << static_cast<const VHandler&>(obj);
    return os;
  }

 private:
  Slice route_key_;
  UType type_;
  Hash hash_;
};

}  // namespace ustore

#endif  //  USTORE_TYPES_CLIENT_VREF_H_
