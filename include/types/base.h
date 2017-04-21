// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_BASE_H_
#define USTORE_TYPES_BASE_H_

#include <cstddef>
#include <memory>

#include "node/node.h"
#include "store/chunk_loader.h"
#include "utils/noncopyable.h"

namespace ustore {
class BaseType : Noncopyable {
  // A genric type for parent class
  // all other types shall inherit from this
 public:
  virtual bool empty() const = 0;

  virtual const Hash hash() const = 0;

 protected:
  explicit BaseType(std::shared_ptr<ChunkLoader> loader) :
      chunk_loader_(std::move(loader)) {}

  virtual ~BaseType() = default;

  // Must be called at the last step of construction
  virtual bool SetNodeForHash(const Hash& hash) = 0;

  std::shared_ptr<ChunkLoader> chunk_loader_;
};

class ChunkableType : public BaseType {
  // A genric type for parent class
  // all other types shall inherit from this
 public:
  inline bool empty() const override {
    return root_node_.get() == nullptr;
  }

  inline const Hash hash() const override {
    CHECK(!empty());
    return root_node_->hash();
  }

 protected:
  explicit ChunkableType(std::shared_ptr<ChunkLoader> loader) noexcept :
      BaseType(loader) {}

  virtual ~ChunkableType() = default;

  std::unique_ptr<const SeqNode> root_node_;
};

}  // namespace ustore

#endif  //  USTORE_TYPES_BASE_H_
