// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_SERVER_SLIST_H_
#define USTORE_TYPES_SERVER_SLIST_H_

#include <vector>

#include "types/ulist.h"

namespace ustore {

class SList : public UList {
 public:
  SList() = default;
  SList(SList&&) = default;
  SList& operator=(SList&&) = default;
  // Load existing SList
  explicit SList(const Hash& root_hash) noexcept;
  // create new SList
  explicit SList(const std::vector<Slice>& elements) noexcept;
  ~SList() = default;

  // entry vector can be empty
  Hash Splice(uint64_t start_idx, uint64_t num_to_delete,
              const std::vector<Slice>& entries) const override;
};
}  // namespace ustore

#endif  //  USTORE_TYPES_SERVER_SLIST_H_
