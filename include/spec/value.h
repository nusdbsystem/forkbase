// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_SPEC_VALUE_H_
#define USTORE_SPEC_VALUE_H_

#include "types/type.h"
#include "spec/blob.h"
#include "spec/slice.h"
#include "utils/logging.h"

namespace ustore {

/*
 * Value is just a wrapper for different types of data.
 * It does not copy data content when initialize.
 */
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

 private:
  UType type_;
  const void* data_ = nullptr;
  size_t size_;
};
}  // namespace ustore

#endif  // USTORE_SPEC_VALUE_H_
