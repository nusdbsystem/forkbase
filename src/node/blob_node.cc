// Copyright (c) 2017 The Ustore Authors.

#include "node/blob_node.h"

#include <cstring>  // for memcpy
#include "utils/logging.h"

namespace ustore {

const ChunkInfo BlobChunker::make(const std::vector<const Segment*>& segments)
    const {
  size_t chunk_num_bytes = 0;
  size_t num_entries = 0;
  for (size_t i = 0; i < segments.size(); i++) {
    chunk_num_bytes += segments[i]->numBytes();
    num_entries += segments[i]->numEntries();
  }
  ustore::Chunk* chunk = new Chunk(ChunkType::kBlob, chunk_num_bytes);
  size_t seg_offset = 0;
  for (const Segment* seg : segments) {
    seg->AppendForChunk(chunk->m_data() + seg_offset);
    seg_offset += seg->numBytes();
  }
  size_t me_num_bytes;
  // TODO(pingcheng): be aware of memory leak, may need unique_ptr for ChunkInfo
  const byte_t* me_data = MetaEntry::Encode(1, num_entries, chunk->hash(),
                                            OrderedKey(0), &me_num_bytes);
  VarSegment* meta_seg = new VarSegment(me_data, me_num_bytes, {0});
  return {chunk, meta_seg};
}

size_t BlobNode::GetIdxForKey(const OrderedKey& key, bool* found) const {
  LOG(FATAL) << "Not Supported to Get Blob Elemeny by Key. ";
  return 0;
}

size_t BlobNode::Copy(size_t start, size_t num_bytes, byte_t* buffer) const {
  size_t len = num_bytes;
  if (start + len > numElements()) {
    LOG(WARNING)
        << "start + len > BlobNode capacity. Will copy until chunk end.";
    len = numElements() - start;
  }
  std::memcpy(buffer, chunk_->data() + start, len);
  return len;
}

}  // namespace ustore
