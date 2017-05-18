// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_SERVER_SLIST_H_
#define USTORE_TYPES_SERVER_SLIST_H_

#include <vector>

#include "types/ulist.h"

namespace ustore {

class SList : public UList {
// UMap for server side
 public:
  // Load an existing map using hash
  explicit SList(const Hash& root_hash) noexcept;

  // create an SList using the initial elements
  explicit SList(const std::vector<Slice>& elements) noexcept;

  SList() = default;

  SList(SList&& rhs) noexcept :
      UList(std::move(rhs)) {}

  SList& operator=(SList&& rhs) noexcept {
    UList::operator=(std::move(rhs));
    return *this;
  }

  // create an empty map
  // construct chunk loader for server
  ~SList() = default;

  // entry vector can be empty
  Hash Splice(uint64_t start_idx, uint64_t num_to_delete,
              const std::vector<Slice>& entries) const override;
};
}  // namespace ustore

#endif  //  USTORE_TYPES_SERVER_SLIST_H_
