// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_SPEC_VALUE_H_
#define USTORE_SPEC_VALUE_H_

#include "types/type.h"
#include "spec/blob.h"
#include "spec/slice.h"

namespace ustore {

/*
 * Value is just a wrapper for different types of data.
 * It does not copy data content when initialize.
 */
class Value {
 public:
  // create value from another value 
  Value(const Value& v);
  // create value with type String 
  explicit Value(const Slice& v);
  // create value with type Blob 
  explicit Value(const Blob& v);
  ~Value();

  inline Type type() const { return type_; }
  const Slice* slice() const { return static_cast<Slice*>(data_); }
  const Blob* blob() const { return static_cast<Blob*>(data_); }

 private:
  Type type_;
  void* data_ = nullptr;
};

}  // namespace ustore
#endif  // USTORE_SPEC_VALUE_H_
