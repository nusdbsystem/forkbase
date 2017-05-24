// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VMETA_H_
#define USTORE_TYPES_CLIENT_VMETA_H_

#include <utility>
#include "spec/db.h"
#include "spec/value.h"
#include "types/ucell.h"
#include "types/client/vblob.h"
#include "types/client/vlist.h"
#include "types/client/vmap.h"
#include "types/client/vstring.h"
#include "utils/noncopyable.h"

namespace ustore {

// TODO(wangsh): modify DB api and remove version in VMeta
class VMeta : private Noncopyable {
 public:
  VMeta(DB* db, ErrorCode code, UCell&& cell)
      : db_(db), code_(code), cell_(std::move(cell)) {}
  VMeta(DB* db, ErrorCode code, Hash&& version)
      : db_(db), code_(code), version_(std::move(version)) {}
  // moveable
  VMeta(VMeta&& other) {
    db_ = other.db_;
    code_ = other.code_;
    std::swap(cell_, other.cell_);
    std::swap(version_, other.version_);
  }
  ~VMeta() = default;

  // moveable
  VMeta& operator=(VMeta&& other) {
    db_ = other.db_;
    code_ = other.code_;
    std::swap(cell_, other.cell_);
    std::swap(version_, other.version_);
    return *this;
  }

  inline const UCell& cell() const { return cell_; }
  inline const Hash& version() const { return version_; }
  inline ErrorCode code() const { return code_; }

  VBlob Blob() const;
  VString String() const;
  VList List() const;
  VMap Map() const;

 private:
  DB* db_;
  ErrorCode code_;
  UCell cell_;
  Hash version_;
};

}  // namespace ustore

#endif  //  USTORE_TYPES_CLIENT_VMETA_H_
