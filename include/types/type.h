// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_TYPE_H_
#define USTORE_TYPES_TYPE_H_

namespace ustore {

typedef unsigned char byte_t;

enum Type : byte_t {
  // Internal types
  kUNode = 0,
  // Primitive types
  kNum = 1,
  kString = 2,
  kBlob = 3,
  // Structured types
  kList = 4,
  kSet = 5,
  kMap = 6
};

}  // namespace ustore
#endif  // USTORE_TYPES_TYPE_H_
