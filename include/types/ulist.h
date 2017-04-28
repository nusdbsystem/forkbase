// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_ULIST_H_
#define USTORE_TYPES_ULIST_H_

#include <memory>
#include <vector>
#include <utility>

#include "types/base.h"
#include "types/iterator.h"

namespace ustore {

class ListIterator : public Iterator {
 public:
  explicit ListIterator(std::unique_ptr<NodeCursor> cursor) :
      Iterator(std::move(cursor)) {}

  // TODO(wangji/pingcheng): const on return value may not be necessary
  const Slice entry() const {
    // Exclude the first four bytes that encode entry len
    size_t len = cursor_->numCurrentBytes() - sizeof(uint32_t);
    auto data = reinterpret_cast<const char*>(cursor_->current()
                                              // Skip the first 4 bytes
                                              + sizeof(uint32_t));
    return Slice(data, len);
  }
};

class UList : public ChunkableType {
 public:
  // For idx > total # of elements
  //    return empty slice
  const Slice Get(size_t idx) const;

  // entry vector can be empty
  virtual const Hash Splice(size_t start_idx, size_t num_to_delete,
                            const std::vector<Slice>& entries) const = 0;

  // Return an iterator that scan from map start
  inline std::unique_ptr<ListIterator> iterator() const {
    CHECK(!empty());
    std::unique_ptr<NodeCursor> cursor(
        NodeCursor::GetCursorByIndex(root_node_->hash(),
                                     0, chunk_loader_.get()));

    return std::unique_ptr<ListIterator>(new ListIterator(std::move(cursor)));
  }

 protected:
  // create an empty map
  // construct chunk loader for server
  explicit UList(std::shared_ptr<ChunkLoader> loader) noexcept :
      ChunkableType(loader) {}
  // construct chunk loader for server
  virtual ~UList() = default;

  bool SetNodeForHash(const Hash& hash) override;

  // friend vector<size_t> diff(const UList& lhs, const UList& rhs);

  // friend vector<size_t> intersect(const UList& lhs, const UList& rhs);
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
  const Hash Splice(size_t start_idx, size_t num_to_delete,
                    const std::vector<Slice>& entries) const override;
};
}  // namespace ustore

#endif  //  USTORE_TYPES_ULIST_H_
