// Copyright (c) 2017 The Ustore Authors.

#include "node/blob_node.h"

#include <cstring>  // for memcpy
#include "utils/logging.h"

namespace ustore {

ChunkInfo BlobChunker::Make(const std::vector<const Segment*>& segments)
    const {
  size_t chunk_num_bytes = 0;
  size_t num_entries = 0;
  for (size_t i = 0; i < segments.size(); i++) {
    chunk_num_bytes += segments[i]->numBytes();
    num_entries += segments[i]->numEntries();
  }
  Chunk chunk(ChunkType::kBlob, chunk_num_bytes);
  size_t seg_offset = 0;
  for (const Segment* seg : segments) {
    seg->AppendForChunk(chunk.m_data() + seg_offset);
    seg_offset += seg->numBytes();
  }
  size_t me_num_bytes;
  std::unique_ptr<const byte_t[]> meta_data(MetaEntry::Encode(
      1, num_entries, chunk.hash(), OrderedKey(0), &me_num_bytes));
  std::unique_ptr<const Segment> meta_seg(
      new VarSegment(std::move(meta_data), me_num_bytes, {0}));
  return {std::move(chunk), std::move(meta_seg)};
}

OrderedKey BlobNode::key(size_t idx) const {
  LOG(FATAL) << "Not Supported";
  return OrderedKey();
}

uint64_t BlobNode::FindIndexForKey(
    const OrderedKey& key, ChunkLoader* loader) const {
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

std::unique_ptr<const Segment> BlobNode::GetSegment(
    size_t start, size_t num_elements) const {
  CHECK_LE(start + num_elements, numEntries());
  std::unique_ptr<const Segment> seg(
      new FixedSegment(data(start), num_elements, 1));
  return seg;
}

}  // namespace ustore
