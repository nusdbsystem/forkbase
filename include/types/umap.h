// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_UMAP_H_
#define USTORE_TYPES_UMAP_H_

#include <memory>
#include <vector>

#include "node/map_node.h"

#include "types/base.h"
#include "types/uiterator.h"

namespace ustore {
class UMap : public ChunkableType {
 public:
  static std::unique_ptr<DuallyDiffKeyIterator> DuallyDiff(
      const UMap& lhs, const UMap& rhs);
  // Use chunk loader to load chunk and read value
  // return empty slice if key not found
  Slice Get(const Slice& key) const;

  // Both Use chunk builder to do splice
  // this kv_items must be sorted in descending order before
  virtual Hash Set(const Slice& key, const Slice& val) const = 0;

  virtual Hash Remove(const Slice& key) const = 0;


  // Return an iterator that scan from List Start
  std::unique_ptr<UIterator> Scan() const;

  // Return an iterator that scan elements that exist in this UMap
  //   and NOT in rhs
  std::unique_ptr<UIterator> Diff(const UMap& rhs) const;

  // Return an iterator that scan elements that both exist in this UMap and rhs
  std::unique_ptr<UIterator> Intersect(const UMap& rhs) const;

 protected:
  explicit UMap(std::shared_ptr<ChunkLoader> loader) noexcept  :
      ChunkableType(loader) {}

  virtual ~UMap() = default;

  bool SetNodeForHash(const Hash& hash) override;

 private:
  class MapIterator : public UIterator {
   public:
    MapIterator(const Hash& root,
                const std::vector<IndexRange>& ranges,
                ChunkLoader* loader) noexcept :
        UIterator(root, ranges, loader) {}

    MapIterator(const Hash& root,
                std::vector<IndexRange>&& ranges,
                ChunkLoader* loader) noexcept :
        UIterator(root, std::move(ranges), loader) {}

    inline uint64_t index() const override {
      LOG(WARNING) << "Index not supported for Map";
      return 0;
    }

  private:
    inline Slice RealValue() const override {
      size_t value_num_bytes = 0;
      const char* value = reinterpret_cast<const char*>(
                              MapNode::value(data(), &value_num_bytes));
      return Slice(value, value_num_bytes);
    }
  };
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
