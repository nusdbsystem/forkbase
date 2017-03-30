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
  Value(const Value& v) {
    type_ = v.type_;
    data_ = v.data_;
  }
  // create value with type String
  explicit Value(const Slice& v) {
    type_ = UType::kString;
    data_ = &v;
  }
  // create value with type Blob
  explicit Value(const Blob& v) {
    type_ = UType::kBlob;
    data_ = &v;
  }
  ~Value() {}

  inline Value& operator=(const Value& v) {
    type_ = v.type_;
    data_ = v.data_;
  }

  inline bool isNull() const { return data_ == nullptr; }
  inline UType type() const { return type_; }
  inline const Slice& slice() const {
    CHECK(type_ == UType::kString);
    return *static_cast<const Slice*>(data_);
  }
  inline const Blob& blob() const {
    CHECK(type_ == UType::kBlob);
    return *static_cast<const Blob*>(data_);
  }

 private:
  UType type_;
  const void* data_ = nullptr;
};
}  // namespace ustore

#endif  // USTORE_SPEC_VALUE_H_
