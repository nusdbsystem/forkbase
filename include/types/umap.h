// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_UMAP_H_
#define USTORE_TYPES_UMAP_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "node/map_node.h"
#include "types/base.h"
#include "types/uiterator.h"

namespace ustore {

class UMap : public ChunkableType {
 public:
  static DuallyDiffKeyIterator DuallyDiff(const UMap& lhs, const UMap& rhs);

  class Iterator : public CursorIterator {
    friend class UMap;
   public:
    inline uint64_t index() const override {
      LOG(WARNING) << "Index not supported for Map";
      return 0;
    }

   private:
    // Only used by UMap
    Iterator(const Hash& root, const std::vector<IndexRange>& ranges,
             ChunkLoader* loader) noexcept
      : CursorIterator(root, ranges, loader) {}

    // Only used by UMap
    Iterator(const Hash& root, std::vector<IndexRange>&& ranges,
             ChunkLoader* loader) noexcept
      : CursorIterator(root, std::move(ranges), loader) {}

    inline Slice RealValue() const override {
      size_t value_num_bytes = 0;
      const char* value = reinterpret_cast<const char*>(
                            MapNode::value(data(), &value_num_bytes));
      return Slice(value, value_num_bytes);
    }
  };

  // Use chunk loader to load chunk and read value
  // return empty slice if key not found
  Slice Get(const Slice& key) const;
  // All use chunk builder to do splice
  virtual Hash Set(const Slice& key, const Slice& val) const = 0;
  virtual Hash Set(const std::vector<Slice>& keys,
                   const std::vector<Slice>& vals) const = 0;

  // Utilities for batch insertion
  inline Hash Insert(const std::map<std::string, std::string>& kvs) const {
    return SetFromStringStringPairs(kvs);
  }
  inline Hash Insert(const std::unordered_map<std::string, std::string>& kvs)
      const {
    return SetFromStringStringPairs(kvs);
  }
  inline Hash Insert(const std::map<std::string, Hash>& kvs) const {
    return SetFromStringHashPairs(kvs);
  }
  inline Hash Insert(const std::unordered_map<std::string, Hash>& kvs) const {
    return SetFromStringHashPairs(kvs);
  }

  virtual Hash Remove(const Slice& key) const = 0;

  // Return an iterator that scan from List Start
  UMap::Iterator Scan() const;
  // Return an iterator that scan elements that exist in this UMap
  //   and NOT in rhs
  UMap::Iterator Diff(const UMap& rhs) const;
  // Return an iterator that scan elements that both exist in this UMap and rhs
  UMap::Iterator Intersect(const UMap& rhs) const;

  friend std::ostream& operator<<(std::ostream& os, const UMap& obj);

 protected:
  UMap() = default;
  UMap(UMap&&) = default;
  UMap& operator=(UMap&&) = default;
  explicit UMap(std::shared_ptr<ChunkLoader> loader) noexcept
    : ChunkableType(loader) {}
  ~UMap() = default;

  bool SetNodeForHash(const Hash& hash) override;

 private:
  // TODO(wangsh): Following template methods cannot contain constexpr value in
  // some g++ versions, as it may cause undefined references during linking.
  // We fixed it by creating a local const buffer.
  // Need to figure out a better solution later.
  static const size_t kHashByteLength;
  template<typename T>
  Hash SetFromStringStringPairs(const T& kvs) const;
  template<typename T>
  Hash SetFromStringHashPairs(const T& kvs) const;
};

template<typename T>
Hash UMap::SetFromStringStringPairs(const T& kvs) const {
  std::vector<Slice> keys, vals;
  for (auto& kv : kvs) {
    keys.emplace_back(kv.first);
    vals.emplace_back(kv.second);
  }
  return Set(keys, vals);
}

template<typename T>
Hash UMap::SetFromStringHashPairs(const T& kvs) const {
  std::vector<Slice> keys, vals;
  for (auto& kv : kvs) {
    keys.emplace_back(kv.first);
    vals.emplace_back(kv.second.value(), kHashByteLength);
  }
  return Set(keys, vals);
}

}  // namespace ustore

#endif  // USTORE_TYPES_UMAP_H_
