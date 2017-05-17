// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VMETA_H_
#define USTORE_TYPES_CLIENT_VMETA_H_

#include <utility>
#include "spec/db.h"
#include "spec/value.h"
#include "types/ucell.h"
#include "types/client/vblob.h"
#include "utils/noncopyable.h"

namespace ustore {

// TODO(wangsh): modify DB api and remove hash in VMeta
class VMeta : private Noncopyable {
 public:
  VMeta(DB2* db, ErrorCode code, UCell&& cell)
      : db_(db), code_(code), cell_(std::move(cell)) {}
  VMeta(DB2* db, ErrorCode code, Hash&& hash)
      : db_(db), code_(code), hash_(std::move(hash)) {}
  // moveable
  VMeta(VMeta&& other) {
    db_ = other.db_;
    code_ = other.code_;
    std::swap(cell_, other.cell_);
    std::swap(hash_, other.hash_);
  }
  ~VMeta() = default;

  // moveable
  VMeta& operator=(VMeta&& other) {
    db_ = other.db_;
    code_ = other.code_;
    std::swap(cell_, other.cell_);
    std::swap(hash_, other.hash_);
    return *this;
  }

  inline const UCell& cell() const { return cell_; }
  inline const Hash& hash() const { return hash_; }
  inline ErrorCode code() const { return code_; }

  VBlob Blob() const;

 private:
  DB2* db_;
  ErrorCode code_;
  UCell cell_;
  Hash hash_;
};

}  // namespace ustore

#endif  //  USTORE_TYPES_CLIENT_VMETA_H_
