// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_BASE_H_
#define USTORE_TYPES_BASE_H_

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "chunk/chunk.h"
#include "hash/hash.h"
#include "node/node.h"
#include "store/chunk_loader.h"
#include "types/type.h"
#include "utils/noncopyable.h"

namespace ustore {
class BaseType : private Noncopyable{
  // A genric type for parent class
  // all other types shall inherit from this
 public:
  explicit BaseType(std::shared_ptr<ChunkLoader> loader) :
      chunk_loader_(std::move(loader)) {}

  virtual ~BaseType() = default;

  virtual bool empty() const = 0;

  virtual const Hash hash() const = 0;

 protected:
  // Must be called at the last step of construction
  virtual bool SetNodeForHash(const Hash& hash) = 0;

  // chunk loader is shared among evolved objects
  std::shared_ptr<ChunkLoader> chunk_loader_;
};

class ChunkableType : public BaseType {
  // A genric type for parent class
  // all other types shall inherit from this
 public:
  explicit ChunkableType(std::shared_ptr<ChunkLoader> loader) :
      BaseType(loader) {}

  virtual ~ChunkableType() = default;

  inline bool empty() const override {
    return root_node_.get() == nullptr;
  }

  inline const Hash hash() const override {
    CHECK(!empty());
    return root_node_->hash();
  }

 protected:
  std::unique_ptr<const SeqNode> root_node_;
};

}  // namespace ustore

#endif  //  USTORE_TYPES_BASE_H_
