// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_TYPE_H_
#define USTORE_TYPES_TYPE_H_

namespace ustore {

typedef unsigned char byte_t;

enum UType : byte_t {
  // Primitive types
  kBool = 0,
  kNum = 1,
  kString = 2,
  kBlob = 3,
  // Structured types
  kList = 4,
  kSet = 5,
  kMap = 6
};

enum ChunkType : byte_t {
  // UCell Chunk
  kCellChunk = 0,
  // Meta SeqNode Chunk
  kMetaChunk = 1,
  // List of Leaf SeqNode Chunk
  kBlobChunk = 2,
  kStringChunk = 3
};

enum ErrorCode : int8_t {
  kOK = 0,
  kUnknownOp = 1,
  kReadFailed = 2,
  kWriteFailed = 3
};

}  // namespace ustore
#endif  // USTORE_TYPES_TYPE_H_
