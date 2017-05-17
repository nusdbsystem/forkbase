// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_SERVER_SMAP_H_
#define USTORE_TYPES_SERVER_SMAP_H_

#include <vector>

#include "types/umap.h"

namespace ustore {

class SMap : public UMap {
 public:
  SMap() = default;
  SMap(SMap&& rhs) = default;
  // Load existing SMap
  explicit SMap(const Hash& root_hash) noexcept;
  // Create new SMap
  // kv_items must be sorted in strict ascending order based on key
  SMap(const std::vector<Slice>& keys,
       const std::vector<Slice>& vals) noexcept;
  ~SMap() = default;

  SMap& operator=(SMap&& rhs) = default;

  // Both Use chunk builder to do splice
  // this kv_items must be sorted in descending order before
  Hash Set(const Slice& key, const Slice& val) const override;
  Hash Remove(const Slice& key) const override;
};

}  // namespace ustore

#endif  // USTORE_TYPES_SERVER_SMAP_H_
