// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VMETA_H_
#define USTORE_TYPES_CLIENT_VMETA_H_

#include <memory>
#include <utility>
#include "types/ucell.h"
#include "types/client/vinit.h"
#include "utils/noncopyable.h"

namespace ustore {

// TODO(wangsh): modify DB api and remove version in VMeta
class VMeta : public VInit, private Moveable {
 public:
  VMeta() = default;
  VMeta(DB* db, UCell&& cell) : VInit(db), cell_(std::move(cell)) {}
  VMeta(DB* db, UCell&& cell, std::shared_ptr<ChunkLoader> loader)
    : VInit(db, loader), cell_(std::move(cell)) {}
  VMeta(VMeta&&) = default;
  VMeta& operator=(VMeta&&) = default;
  ~VMeta() = default;

  UType type() const override { return cell_.type(); }
  Hash dataHash() const override { return cell_.dataHash(); }
  Slice partitionKey() const override { return cell_.key(); }

  inline const UCell& cell() const { return cell_; }

  VString String() const override {
    if (!empty() && type() == UType::kString) return VString(cell_);
    LOG(WARNING) << "Get empty VString, actual type: " << type();
    return VString();
  }

  friend std::ostream& operator<<(std::ostream& os, const VMeta& obj) {
    os << static_cast<const VInit&>(obj);
    return os;
  }

 private:
  UCell cell_;
};

}  // namespace ustore

#endif  //  USTORE_TYPES_CLIENT_VMETA_H_
