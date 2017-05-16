// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPES_TYPE_H_
#define USTORE_TYPES_TYPE_H_

#include <algorithm>
#include <cstdint>
#include <string>


namespace ustore {

typedef unsigned char byte_t;
typedef uint16_t key_size_t;

/*
 * Supported data types
 */
enum class UType : byte_t {
  kUnknown = 0,
  // Primitive types
  kBool = 1,
  kNum = 2,
  kString = 3,
  kBlob = 4,
  // Structured types
  kList = 5,
  kSet = 6,
  kMap = 7,

  First = kBool,
  Last = kMap
};

/*
 * For internal usage
 * Chunk types in chunk store
 */
enum class ChunkType : byte_t {
  // used in chunk store
  kNull = 0,
  // UCell Chunk
  kCell = 1,
  // Meta SeqNode Chunk
  kMeta = 2,
  // Instances of Leaf SeqNode Chunk
  kBlob = 3,
  kString = 4,
  kMap = 5,
  kList = 6,

  First = kCell,
  Last = kList,

  // Indicate the validity
  kInvalid = 99
};

static inline bool IsChunkValid(ChunkType type) noexcept {
  return type == ChunkType::kNull
         || type == ChunkType::kCell
         || type == ChunkType::kMeta
         || type == ChunkType::kBlob
         || type == ChunkType::kString
         || type == ChunkType::kMap
         || type == ChunkType::kList
         || type == ChunkType::kInvalid;
}

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
  kFailedCreateSBlob = 11,
  kFailedCreateSString = 12,
  kFailedCreateSList = 13,
  kFailedCreateSMap = 14,
  kInconsistentKey = 15,
  kInvalidValue2 = 16,
  kFailedModifySBlob = 17,
  kFailedModifySList = 18,
  kFailedModifySMap = 19,
  kIndexOutOfRange = 20,
  kTypeMismatch = 21, 
  kKeyNotExists = 22, 
  kKeyExists = 23
};

}  // namespace ustore

namespace std {

template<>
struct hash<::ustore::ChunkType> {
  size_t operator()(const ::ustore::ChunkType& key) const {
    return static_cast<std::size_t>(key);
  }
};
}  // namespace std

#endif  // USTORE_TYPES_TYPE_H_
