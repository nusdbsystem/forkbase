// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_UMAP_H_
#define USTORE_TYPES_UMAP_H_

#include <memory>
#include <vector>

#include "types/base.h"
#include "types/iterator.h"

namespace ustore {

class KVIterator : public Iterator {
 public:
  explicit KVIterator(std::unique_ptr<NodeCursor> cursor);

  const Slice key() const;
  const Slice value() const;
};

class UMap : public ChunkableType {
 public:
  // Use chunk loader to load chunk and read value
  // return empty slice if key not found
  Slice Get(const Slice& key) const;

  // Both Use chunk builder to do splice
  // this kv_items must be sorted in descending order before
  virtual Hash Set(const Slice& key, const Slice& val) const = 0;

  virtual Hash Remove(const Slice& key) const = 0;

  // Return an iterator that scan from map start
  std::unique_ptr<KVIterator> iterator() const;

 protected:
  explicit UMap(std::shared_ptr<ChunkLoader> loader) noexcept  :
      ChunkableType(loader) {}

  virtual ~UMap() = default;

  bool SetNodeForHash(const Hash& hash) override;
};

class SMap : public UMap {
// UMap for server side
 public:
  // Load an existing map using hash
  explicit SMap(const Hash& root_hash) noexcept;

  // create an SMap using the kv_items
  // kv_items must be sorted in strict ascending order based on key
  SMap(const std::vector<Slice>& keys,
       const std::vector<Slice>& vals) noexcept;

  ~SMap() = default;

  // Both Use chunk builder to do splice
  // this kv_items must be sorted in descending order before
  Hash Set(const Slice& key, const Slice& val) const override;

  Hash Remove(const Slice& key) const override;
};
}  // namespace ustore

#endif  // USTORE_TYPES_UMAP_H_
