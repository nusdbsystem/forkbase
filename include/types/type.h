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
  // UCell Chunk
  kCell = 0,
  // Meta SeqNode Chunk
  kMeta = 1,
  // Instances of Leaf SeqNode Chunk
  kBlob = 2,
  kString = 3
};

}  // namespace ustore

#endif  // USTORE_TYPES_TYPE_H_
