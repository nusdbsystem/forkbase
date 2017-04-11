// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_TYPE_H_
#define USTORE_TYPES_TYPE_H_

namespace ustore {

typedef unsigned char byte_t;

/*
 * Supported data types
 */
enum class UType : byte_t {
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

/*
 * For internal usage
 * Chunk types in chunk store
 */
enum class ChunkType : byte_t {
  kNull = 0, // used in chunk store
  // UCell Chunk
  kCell = 1,
  // Meta SeqNode Chunk
  kMeta = 2,
  // Instances of Leaf SeqNode Chunk
  kBlob = 3,
  kString = 4,
  kInvalid = 5 // indicate the validity
};

/*
 * Worker error code returned to user
 */
enum class ErrorCode : byte_t {
  kOK = 0,
  kUnknownOp = 1,
  kInvalidRange = 2,
  kReadFailed = 3,
  kWriteFailed = 4,
  kBranchExists = 5,
  kBranchNotExists = 6,
  kUCellNotfound = 7,
  kTypeUnsupported = 8,
  kInvalidParameters = 9,
  kFailedCreateUCell = 10,
  kFailedCreateUBlob = 11,
  kFailedCreateUString = 12
};

}  // namespace ustore

#endif  // USTORE_TYPES_TYPE_H_
