// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_SPEC_VALUE_H_
#define USTORE_SPEC_VALUE_H_

#include <iostream>
#include <memory>
#include <vector>
#include "hash/hash.h"
#include "types/type.h"
#include "spec/blob.h"
#include "spec/slice.h"
#include "utils/logging.h"

namespace ustore {

/*
 * Value is just a wrapper for different types of data.
 * It does not copy data content when initialize.
 */
// Value is deprecated in v0.2
class Value {
 public:
  // create empty value
  Value() {}
  // create value from another value
  Value(const Value& v) : type_(v.type_), data_(v.data_), size_(v.size_) {}
  // create value with type String
  explicit Value(const Slice& v)
      : type_(UType::kString), data_(v.data()), size_(v.len()) {}
  // create value with type Blob
  explicit Value(const Blob& v)
      : type_(UType::kBlob), data_(v.data()), size_(v.size()) {}
  ~Value() {}

  inline Value& operator=(const Value& v) {
    type_ = v.type_;
    data_ = v.data_;
    size_ = v.size_;
    return *this;
  }

  inline bool empty() const { return data_ == nullptr; }
  inline UType type() const { return type_; }
  inline Slice slice() const {
    CHECK(type_ == UType::kString);
    return Slice(static_cast<const char*>(data_), size_);
  }
  inline Blob blob() const {
    CHECK(type_ == UType::kBlob);
    return Blob(static_cast<const byte_t*>(data_), size_);
  }

  // ensure to call Release when finish using a value returned from worker
  inline void Release() {
    if (empty()) return;
    switch (type_) {
      case UType::kString:
        delete[] slice().data();
        break;
      case UType::kBlob:
        delete[] blob().data();
        break;
    }
    data_ = nullptr;
  }

  friend inline bool operator==(const Value& lhs, const Value& rhs) noexcept {
    CHECK(lhs.data_ != nullptr && rhs.data_ != nullptr);
    return lhs.type_ == rhs.type_ && lhs.size_ && rhs.size_ &&
           std::memcmp(lhs.data_, rhs.data_, lhs.size_) == 0;
  }

  friend inline bool operator!=(const Value& lhs, const Value& rhs) noexcept {
    return !operator==(lhs, rhs);
  }

  friend inline std::ostream& operator<<(std::ostream& os, const Value& obj) {
    switch (obj.type_) {
      case UType::kBlob:
        os << obj.blob() << " <Blob>";
        break;
      case UType::kString:
        os << obj.slice() << " <String>";
        break;
      default:
        LOG(WARNING) << "Unsupported data type: "
                     << static_cast<int>(obj.type_);
    }
    return os;
  }

 private:
  UType type_;
  const void* data_ = nullptr;
  size_t size_;
};

// This struct is generalized to inserts/updates any UTypes
struct Value2 {
  // Type of the value
  UType type;
  // Hash::kNull if it is a new insertion
  // Otherwise, it is an update from the content that has the Hash
  Hash base;
  // Only used for an update
  // Indicate where to start the insertion/deletion
  size_t pos;
  // Number of deletions
  size_t dels;
  // Content of Insertions
  // size = 1 for Blob/String
  // size > 1 for Map/List
  std::vector<Slice> vals;
  // Only used my Map, and has keys.size() = vals.size()
  std::vector<Slice> keys;
};

}  // namespace ustore

#endif  // USTORE_SPEC_VALUE_H_
