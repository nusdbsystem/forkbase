// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_SERVER_SMAP_H_
#define USTORE_TYPES_SERVER_SMAP_H_

#include <memory>
#include <vector>

#include "chunk/chunk_writer.h"
#include "types/umap.h"

namespace ustore {

class SMap : public UMap {
  friend class ChunkableTypeFactory;
 public:
  SMap(SMap&&) = default;
  SMap& operator=(SMap&&) = default;
  ~SMap() = default;

  // Both Use chunk builder to do splice
  // this kv_items must be sorted in descending order before
  Hash Set(const Slice& key, const Slice& val) const override;
  Hash Remove(const Slice& key) const override;

 protected:
  // Load existing SMap
  SMap(const Hash& root_hash,
       std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer) noexcept;
  // Create new SMap
  // kv_items must be sorted in strict ascending order based on key
  SMap(const std::vector<Slice>& keys, const std::vector<Slice>& vals,
       std::shared_ptr<ChunkLoader> loader, ChunkWriter* writer) noexcept;

 private:
  ChunkWriter* chunk_writer_;
};

}  // namespace ustore

#endif  // USTORE_TYPES_SERVER_SMAP_H_
