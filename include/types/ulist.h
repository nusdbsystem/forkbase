// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_ULIST_H_
#define USTORE_TYPES_ULIST_H_

#include <memory>
#include <vector>
#include <utility>

#include "node/list_node.h"
#include "types/base.h"
#include "types/uiterator.h"

namespace ustore {
class UList : public ChunkableType {
 public:
  // For idx > total # of elements
  //    return empty slice
  Slice Get(uint64_t idx) const;

  // entry vector can be empty
  virtual Hash Splice(uint64_t start_idx, uint64_t num_to_delete,
                      const std::vector<Slice>& entries) const = 0;

  Hash Delete(uint64_t start_idx, uint64_t num_to_delete) const;

  Hash Insert(uint64_t start_idx, const std::vector<Slice>& entries) const;

  Hash Append(const std::vector<Slice>& entries) const;

  // Return an iterator that scan from List Start
  std::unique_ptr<UIterator> Scan() const;

  // Return an iterator that scan elements that exist in this Ulist
  //   and NOT in rhs
  std::unique_ptr<UIterator> Diff(const UList& rhs) const;

  // Return an iterator that scan elements that both exist in this Ulist and rhs
  std::unique_ptr<UIterator> Intersect(const UList& rhs) const;

 protected:
  // create an empty map
  // construct chunk loader for server
  explicit UList(std::shared_ptr<ChunkLoader> loader) noexcept :
      ChunkableType(loader) {}
  // construct chunk loader for server
  virtual ~UList() = default;

  bool SetNodeForHash(const Hash& hash) override;

 private:
  class ListIterator : public UIterator {
   public:
    ListIterator(const Hash& root,
                 const std::vector<IndexRange>& ranges,
                 ChunkLoader* loader) noexcept :
        UIterator(root, ranges, loader) {}

    ListIterator(const Hash& root,
                 std::vector<IndexRange>&& ranges,
                 ChunkLoader* loader) noexcept :
        UIterator(root, std::move(ranges), loader) {}

    inline Slice key() const override {
      LOG(WARNING) << "Key not supported for list";
      return Slice(nullptr, 0);
    }

  private:
    inline Slice RealValue() const override {
      return ListNode::Decode(data());
    }
  };
};

class SList : public UList {
// UMap for server side
 public:
  // Load an existing map using hash
  explicit SList(const Hash& root_hash) noexcept;

  // create an SList using the initial elements
  explicit SList(const std::vector<Slice>& elements) noexcept;

  // create an empty map
  // construct chunk loader for server
  ~SList() = default;

  // entry vector can be empty
  Hash Splice(uint64_t start_idx, uint64_t num_to_delete,
              const std::vector<Slice>& entries) const override;
};
}  // namespace ustore

#endif  //  USTORE_TYPES_ULIST_H_
