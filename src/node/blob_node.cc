
// Copyright (c) 2017 The Ustore Authors.

#include "node/blob_node.h"

#include <cstring>  // for memcpy
#include "utils/logging.h"

namespace ustore {

const ChunkInfo BlobNode::MakeChunk(
    const std::vector<const byte_t*>& element_data,
    const std::vector<size_t>& element_num_bytes) {
  size_t num_entries = element_num_bytes.size();
  uint64_t total_num_bytes = 0;
  for (const size_t num_bytes : element_num_bytes) {
    total_num_bytes += num_bytes;
  }
  Chunk* chunk = new Chunk(ChunkType::kBlob, total_num_bytes);
  size_t byte_offset = 0;
  for (size_t idx = 0; idx < num_entries; idx++) {
    std::memcpy(chunk->m_data() + byte_offset, element_data[idx],
                element_num_bytes[idx]);
    byte_offset += element_num_bytes[idx];
  }
  size_t me_num_bytes;
  // key is useless for blob
  // Any fake key to occupies this field
  const byte_t* me_data = MetaEntry::Encode(1, total_num_bytes, chunk->hash(),
                                            OrderedKey(0), &me_num_bytes);
  return {chunk, {me_data, me_num_bytes}};
}

size_t BlobNode::GetIdxForKey(const OrderedKey& key, bool* found) const {
  LOG(FATAL) << "Not Supported to Get Blob Elemeny by Key. ";
  return 0;
}

size_t BlobNode::Copy(size_t start, size_t num_bytes, byte_t* buffer) const {
  size_t len = num_bytes;
  if (start + len > this->numElements()) {
    LOG(WARNING)
      << "start + len > BlobNode capacity. Will copy until chunk end. ";
    len = this->numElements() - start;
  }
  std::memcpy(buffer, chunk_->data() + start, len);
  return len;
}

}  // namespace ustore
