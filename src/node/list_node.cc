// Copyright (c) 2017 The Ustore Authors.

#include "node/list_node.h"

#include <cstring>  // for memcpy

#include "hash/hash.h"
#include "node/orderedkey.h"
#include "utils/logging.h"
#include "utils/debug.h"

namespace ustore {

ChunkInfo ListChunker::make(const std::vector<const Segment*>& segments) const {
  // Caculate the total number of entries and bytes for all segments
  CHECK_GT(segments.size(), 0);
  size_t num_entries = 0;
  size_t chunk_num_bytes = sizeof(uint32_t);
  for (const auto& seg : segments) {
    CHECK(!seg->empty());
    num_entries += seg->numEntries();
    chunk_num_bytes += seg->numBytes();
  }

  std::unique_ptr<Chunk> chunk(new Chunk(ChunkType::kList, chunk_num_bytes));
  uint32_t unum_entries = static_cast<uint32_t>(num_entries);
  std::memcpy(chunk->m_data(), &unum_entries, sizeof(uint32_t));
  size_t seg_offset = sizeof(uint32_t);

  // Concat segments into chunk one by one
  for (const auto& seg : segments) {
    seg->AppendForChunk(chunk->m_data() + seg_offset);
    seg_offset += seg->numBytes();
  }

  size_t me_num_bytes;
  // For List, we pad a useless orderkey for create meta_entry
  OrderedKey paddingKey(0);
  std::unique_ptr<const byte_t[]> meta_data(MetaEntry::Encode(
      1, num_entries, chunk->hash(), paddingKey, &me_num_bytes));

  std::unique_ptr<const Segment> meta_seg(
      new VarSegment(std::move(meta_data), me_num_bytes, {0}));

  return {std::move(chunk), std::move(meta_seg)};
}

std::unique_ptr<const Segment> ListNode::encode(
    const std::vector<Slice>& elements) {
  std::vector<size_t> offsets;
  size_t offset = 0;
  for (const auto& element : elements) {
    offsets.push_back(offset);
    // Leave the first 4 bytes to encode element size
    offset += sizeof(uint32_t) + element.len();
  }
  size_t seg_num_bytes = offset;

  byte_t* seg_data = new byte_t[seg_num_bytes];

  offset = 0;
  for (const auto& element : elements) {
    // Count the first bytes for element sizee
    uint32_t usize = static_cast<uint32_t>(element.len() + sizeof(uint32_t));

    std::memcpy(seg_data + offset,
                &usize, sizeof(uint32_t));

    std::memcpy(seg_data + offset + sizeof(uint32_t),
                element.data(), element.len());

    offset += sizeof(uint32_t) + element.len();
  }

  const Segment* seg =
      new VarSegment(std::unique_ptr<const byte_t[]>(seg_data),
                     seg_num_bytes, std::move(offsets));

  return std::unique_ptr<const Segment>(seg);
}

const Slice ListNode::decode(const byte_t* data) {
  size_t element_size = static_cast<size_t>(
                            *reinterpret_cast<const uint32_t*>(data))
                        // does not count the first 4 bytes
                        - sizeof(uint32_t);

  const char* cdata = reinterpret_cast<const char*>(data + sizeof(uint32_t));

  return Slice(cdata, element_size);
}

const byte_t* ListNode::data(size_t idx) const {
  CHECK_LT(idx, numEntries());

  size_t offset = offsets_[idx];
  return chunk_->data() + offset;
}

size_t ListNode::len(size_t idx) const {
  DCHECK_LT(idx, numEntries());

  size_t preOffset = 0;
  if (idx == numEntries() - 1) {
    preOffset = chunk_->capacity();
  } else {
    preOffset = offsets_[idx + 1];
  }

  return preOffset - offsets_[idx];
}

size_t ListNode::numEntries() const {
  return static_cast<size_t>(
             *reinterpret_cast<const uint32_t*>(chunk_->data()));
}

void ListNode::PrecomputeOffsets() {
  CHECK(offsets_.empty());
  // iterate all entries in ListNode and accumulate offset
  // Skip num_entries field (4 bytes) at ListNode head
  size_t byte_offset = sizeof(uint32_t);
  for (size_t i = 0; i < numEntries(); i++) {
    offsets_.push_back(byte_offset);
    size_t entry_num_bytes = static_cast<size_t>(
         *reinterpret_cast<const uint32_t*>(chunk_->data() + byte_offset));
    byte_offset += entry_num_bytes;
  }
}

const OrderedKey ListNode::key(size_t idx) const {
  LOG(FATAL) << "Not Supported";
  return OrderedKey();
}

size_t ListNode::GetIdxForKey(const OrderedKey& key,
                             bool* found) const {
  LOG(FATAL) << "Not Supported Yet";
  return 0;
}

size_t ListNode::Copy(size_t start, size_t num_bytes, byte_t* buffer) const {
  LOG(FATAL) << "Not Supported Yet";
  return 0;
}
size_t ListNode::GetLength(size_t start, size_t end) const {
  LOG(FATAL) << "Not Supported Yet";
  return 0;
}
}  // namespace ustore
