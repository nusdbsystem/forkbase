// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_CLIENT_VOBJECT_H_
#define USTORE_TYPES_CLIENT_VOBJECT_H_

#include <istream>
#include "spec/value.h"

namespace ustore {

class VObject {
 public:
  VObject(): useStream_(false) {}
  virtual ~VObject() = default;

  bool empty() const { return buffer_.type == UType::kUnknown; }
  const Value& value() const { return buffer_; }
  std::istream* dataStream() const { return dataStream_; }
  bool useStream() const { return useStream_; }

  void SetContext(Slice ctx) { buffer_.ctx = ctx; }
  void Clear() { buffer_ = {}; }

 protected:
  mutable Value buffer_;
  mutable bool useStream_;
  mutable std::istream* dataStream_;
};

}  // namespace ustore

#endif  //  USTORE_TYPES_CLIENT_VOBJECT_H_
