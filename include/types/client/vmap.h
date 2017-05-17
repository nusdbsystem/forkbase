// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VMAP_H_
#define USTORE_TYPES_CLIENT_VMAP_H_

#include <memory>
#include <vector>
#include "types/client/vvalue.h"
#include "types/umap.h"

namespace ustore {

class VMap : public UMap, public Buffered {
 public:
  ~VMap() = default;

  Hash Set(const Slice& key, const Slice& val) const override;
  Hash Remove(const Slice& key) const override;

 protected:
  // Load an existing VMap
  VMap(std::shared_ptr<ChunkLoader>, const Hash& root_hash) noexcept;
  // Create a new VMap
  VMap(std::shared_ptr<ChunkLoader>, const std::vector<Slice>& keys,
       const std::vector<Slice>& vals) noexcept;
};

}  // namespace ustore

#endif  // USTORE_TYPES_CLIENT_VMAP_H_
