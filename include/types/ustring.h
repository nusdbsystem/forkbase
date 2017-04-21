// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_USTRING_H_
#define USTORE_TYPES_USTRING_H_

#include "node/string_node.h"
#include "types/base.h"

namespace ustore {
class UString : public BaseType {
 public:

  inline bool empty() const override {
    return this->node_.get() == nullptr;
  }

  inline const Hash hash() const override {
    CHECK(!empty());
    return node_->hash();
  }

  inline size_t len() const { return node_->len(); }
  // copy string contents to buffer
  //   return string length
  inline const size_t data(byte_t* buffer) const { return node_->Copy(buffer); }

  // pointer to orignal data
  inline const byte_t* data() const { return node_->Read(); }

 protected:
  explicit UString(std::shared_ptr<ChunkLoader> loader) noexcept :
      BaseType(loader) {}

  ~UString() = default;

  bool SetNodeForHash(const Hash& hash) override;

 private:
  // Responsible to remove during destructing
  std::unique_ptr<const StringNode> node_;
};

class SString : public UString {
 public:
  // Load an existing sstring
  explicit SString(const Hash& hash) noexcept;

  // Creata a new sstring
  explicit SString(const Slice& data) noexcept;

  ~SString() = default;

  SString& operator=(SString&& rhs) = default;
};

}  // namespace ustore
#endif  // USTORE_TYPES_USTRING_H_
