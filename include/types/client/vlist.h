// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VLIST_H_
#define USTORE_TYPES_CLIENT_VLIST_H_

#include "types/ulist.h"

namespace ustore {
class VList : public UList, private Noncopyable {
// UMap for server side
 public:
  // create an empty map
  // construct chunk loader for client
  VList();
  ~VList() = default;
  // Send message to server

 private:
};
}  // namespace ustore
