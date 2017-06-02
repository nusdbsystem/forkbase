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
  VMeta(DB* db, UCell&& cell) : db_(db), cell_(std::move(cell)) {}
  // moveable
  VMeta(VMeta&& other) {
    db_ = other.db_;
    std::swap(cell_, other.cell_);
  }
  ~VMeta() = default;

  // moveable
  VMeta& operator=(VMeta&& other) {
    db_ = other.db_;
    std::swap(cell_, other.cell_);
    return *this;
  }

  inline const UCell& cell() const { return cell_; }

  VBlob Blob() const;
  VString String() const;
  VList List() const;
  VMap Map() const;

  friend std::ostream& operator<<(std::ostream& os, const VMeta& obj);

 private:
  DB* db_;
  UCell cell_;
};

}  // namespace ustore

#endif  //  USTORE_TYPES_CLIENT_VMETA_H_
