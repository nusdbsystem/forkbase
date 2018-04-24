// Copyright (c) 2017 The Ustore Authors.

#include "node/node.h"

#include <cstring>  // for memcpy
#include "hash/hash.h"
#include "node/blob_node.h"
#include "node/map_node.h"
#include "node/meta_node.h"
#include "node/set_node.h"
#include "node/list_node.h"
#include "node/orderedkey.h"
#include "utils/logging.h"

namespace ustore {

// utility function for cursor usage
std::unique_ptr<const SeqNode>
    SeqNode::CreateFromChunk(const Chunk* chunk) {
  CHECK_NE(chunk, nullptr);
  switch (chunk->type()) {
    case ChunkType::kMeta:
      return std::unique_ptr<SeqNode>(new MetaNode(chunk));
    case ChunkType::kBlob:
      return std::unique_ptr<SeqNode>(new BlobNode(chunk));
    case ChunkType::kMap:
      return std::unique_ptr<SeqNode>(new MapNode(chunk));
    case ChunkType::kSet:
      return std::unique_ptr<SeqNode>(new SetNode(chunk));
    case ChunkType::kList:
      return std::unique_ptr<SeqNode>(new ListNode(chunk));
    default:
      LOG(FATAL) << "Other Non-chunkable Node Not Supported!";
      return nullptr;
  }
}

const byte_t* MetaEntry::Encode(uint32_t num_leaves, uint64_t num_elements,
                                const Hash& data_hash, const OrderedKey& key,
                                size_t* encode_len) {
  uint32_t num_bytes = kKeyOffset + sizeof(bool) + key.numBytes();
  byte_t* data = new byte_t[num_bytes];
  std::memcpy(data + kNumBytesOffset, &num_bytes, sizeof(uint32_t));
  std::memcpy(data + kNumLeavesOffset, &num_leaves, sizeof(uint32_t));
  std::memcpy(data + kNumElementsOffset, &num_elements, sizeof(uint64_t));
  std::memcpy(data + kHashOffset, data_hash.value(), Hash::kByteLength);
  *(data + kKeyOffset) = static_cast<byte_t>(key.byValue());
  key.Encode(data + kKeyOffset + sizeof(bool));

  *encode_len = num_bytes;
  return data;
}

OrderedKey MetaEntry::orderedKey() const {
  // remaining bytes of MetaEntry are for ordered key
  //   skip the by_value field
  size_t key_num_bytes = numBytes() - kKeyOffset - sizeof(bool);
  bool by_value = *(reinterpret_cast<const bool*>(data_ + kKeyOffset));
  return OrderedKey(by_value, data_ + kKeyOffset + sizeof(bool), key_num_bytes);
}

size_t MetaEntry::numBytes() const {
  return *(reinterpret_cast<const uint32_t*>(data_ + kNumBytesOffset));
}

uint32_t MetaEntry::numLeaves() const {
  return *(reinterpret_cast<const uint32_t*>(data_ + kNumLeavesOffset));
}

uint64_t MetaEntry::numElements() const {
  return *(reinterpret_cast<const uint64_t*>(data_ + kNumElementsOffset));
}

Hash MetaEntry::targetHash() const { return Hash(data_ + kHashOffset); }

}  // namespace ustore
