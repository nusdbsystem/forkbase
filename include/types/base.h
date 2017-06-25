// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_BASE_H_
#define USTORE_TYPES_BASE_H_

#include <cstddef>
#include <memory>
#include <utility>

#include "chunk/chunk_loader.h"
#include "node/node.h"
#include "utils/noncopyable.h"

namespace ustore {

// A genric type for all utypes
class BaseType : private Noncopyable {
 public:
  virtual bool empty() const = 0;

 protected:
  BaseType() = default;
  BaseType(BaseType&& rhs) noexcept {}
  virtual ~BaseType() = default;
  // move assignment
  BaseType& operator=(BaseType&& rhs) noexcept { return *this; }
};

class ChunkableType : public BaseType {
  // A genric type for parent class
  // all other types shall inherit from this
 public:
  inline bool empty() const override { return root_node_.get() == nullptr; }
  inline const Hash hash() const {
    return empty() ? Hash() : root_node_->hash();
  }
  inline uint64_t numElements() const {
    CHECK(!empty());
    return root_node_->numElements();
  }

 protected:
  ChunkableType() = default;
  ChunkableType(ChunkableType&& rhs) = default;
  explicit ChunkableType(std::shared_ptr<ChunkLoader> loader) noexcept :
      chunk_loader_(std::move(loader)) {}
  virtual ~ChunkableType() = default;
  // move assignment
  ChunkableType& operator=(ChunkableType&& rhs) = default;

  // Must be called at the last step of construction
  virtual bool SetNodeForHash(const Hash& hash) = 0;

  std::shared_ptr<ChunkLoader> chunk_loader_;
  std::unique_ptr<const SeqNode> root_node_;
};

}  // namespace ustore

#endif  //  USTORE_TYPES_BASE_H_
