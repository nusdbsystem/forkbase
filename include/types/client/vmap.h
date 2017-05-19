// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VMAP_H_
#define USTORE_TYPES_CLIENT_VMAP_H_

#include <memory>
#include <vector>
#include "types/client/vobject.h"
#include "types/umap.h"

namespace ustore {

class VMap : public UMap, public VObject {
  friend class VMeta;

 public:
  VMap() = default;
  VMap(VMap&& rhs) = default;
  // Create new VMap
  VMap(const std::vector<Slice>& keys, const std::vector<Slice>& vals) noexcept;
  ~VMap() = default;

  VMap& operator=(VMap&& rhs) = default;

  Hash Set(const Slice& key, const Slice& val) const override;
  Hash Remove(const Slice& key) const override;

 protected:
  // Load an existing VMap
  VMap(std::shared_ptr<ChunkLoader>, const Hash& root_hash) noexcept;
};

}  // namespace ustore

#endif  // USTORE_TYPES_CLIENT_VMAP_H_
