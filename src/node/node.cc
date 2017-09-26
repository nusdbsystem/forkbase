// Copyright (c) 2017 The Ustore Authors.

#include "node/node.h"

#include <cstring>  // for memcpy
#include "hash/hash.h"
#include "node/blob_node.h"
#include "node/map_node.h"
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

ChunkInfo MetaChunker::Make(const std::vector<const Segment*>& segments)
    const {
  size_t chunk_num_bytes = sizeof(uint32_t);
  size_t num_entries = 0;
  for (size_t i = 0; i < segments.size(); i++) {
    chunk_num_bytes += segments[i]->numBytes();
    num_entries += segments[i]->numEntries();
  }

  Chunk chunk(ChunkType::kMeta, chunk_num_bytes);
  // encode num_entries
  std::memcpy(chunk.m_data(), &num_entries, sizeof(uint32_t));

  uint32_t total_num_leaves = 0;
  uint32_t total_num_elements = 0;

  const MetaEntry* pre_me = nullptr;
  size_t seg_offset = sizeof(uint32_t);
  for (const Segment* seg : segments) {
    for (size_t idx = 0; idx < seg->numEntries(); idx++) {
      const MetaEntry* me = new MetaEntry(seg->entry(idx));
      if (pre_me != nullptr) {
        CHECK(pre_me->orderedKey() <= me->orderedKey())
            << "MetaEntry shall be in non-descending order.";
      }
      total_num_elements += me->numElements();
      total_num_leaves += me->numLeaves();
      delete pre_me;
      pre_me = me;
    }
    seg->AppendForChunk(chunk.m_data() + seg_offset);
    seg_offset += seg->numBytes();
  }

  const OrderedKey key(pre_me->orderedKey());
  delete pre_me;
  size_t me_num_bytes;
  std::unique_ptr<const byte_t[]> meta_data(MetaEntry::Encode(
      total_num_leaves, total_num_elements, chunk.hash(), key, &me_num_bytes));
  std::unique_ptr<const Segment> meta_seg(
      new VarSegment(std::move(meta_data), me_num_bytes, {0}));
  return {std::move(chunk), std::move(meta_seg)};
}

size_t MetaNode::numEntries() const {
  // read the first 4 bytes of chunk data as a uint32_t
  return *reinterpret_cast<const uint32_t*>(chunk_->data());
}

uint64_t MetaNode::numElements() const {
  return numElementsUntilEntry(numEntries());
}

uint32_t MetaNode::numLeaves() const {
  uint32_t total_num_leaves = 0;
  for (size_t entry_idx = 0; entry_idx < numEntries(); ++entry_idx) {
    MetaEntry me(chunk_->data() + entryOffset(entry_idx));
    total_num_leaves += me.numLeaves();
  }
  return total_num_leaves;
}

uint64_t MetaNode::numElementsUntilEntry(size_t entry_idx) const {
  CHECK_GE(entry_idx, size_t(0));
  CHECK_LE(entry_idx, numEntries());
  // iterate all MetaEntries in MetaNode and sum up their elements
  // Skip num_entries field (4 bytes) at MetaNode head
  size_t byte_offset = sizeof(uint32_t);
  uint64_t total_num_elements = 0;
  for (size_t i = 0; i < entry_idx; i++) {
    MetaEntry entry(chunk_->data() + byte_offset);
    total_num_elements += entry.numElements();
    size_t entry_len = entry.numBytes();
    byte_offset += entry_len;
  }
  return total_num_elements;
}

const byte_t* MetaNode::data(size_t idx) const {
  // Skip num_entries field (4 bytes) at MetaNode head
  size_t byte_offset = entryOffset(idx);
  return chunk_->data() + byte_offset;
}

OrderedKey MetaNode::key(size_t idx) const {
  MetaEntry me(data(idx));
  return me.orderedKey();
}

size_t MetaNode::len(size_t idx) const {
  CHECK_GE(idx, size_t(0));
  CHECK_LT(idx, numEntries());
  // iterate all MetaEntries in MetaNode and accumulate offset
  // Skip num_entries field (4 bytes) at MetaNode head
  size_t byte_offset = entryOffset(idx);
  MetaEntry entry(chunk_->data() + byte_offset);
  return entry.numBytes();
}


uint64_t MetaNode::FindIndexForKey(const OrderedKey& key,
                                   ChunkLoader* loader) const {
  uint64_t num_elements_sum = 0;
  for (const size_t offset : offsets_) {
    MetaEntry entry(chunk_->data() + offset);

    if (key <= entry.orderedKey()) {
       const Chunk* chunk = loader->Load(entry.targetHash());
       auto seq_node = SeqNode::CreateFromChunk(chunk);
       return num_elements_sum + seq_node->FindIndexForKey(key, loader);
    }

     num_elements_sum += entry.numElements();
  }
  return num_elements_sum;
 }

uint64_t MetaNode::entryOffset(size_t idx) const {
  // make sure 0 <= idx < numElements()
  CHECK_GE(idx, size_t(0));
  CHECK_LT(idx, numEntries());

  return offsets_[idx];
}

void MetaNode::PrecomputeOffset() {
  CHECK(offsets_.empty());
  // iterate all MetaEntries in MetaNode and accumulate offset
  // Skip num_entries field (4 bytes) at MetaNode head
  size_t byte_offset = sizeof(uint32_t);
  for (size_t i = 0; i < numEntries(); i++) {
    offsets_.push_back(byte_offset);
    MetaEntry entry(chunk_->data() + byte_offset);
    byte_offset += entry.numBytes();
  }
}

Hash MetaNode::GetChildHashByIndex(size_t element_idx,
                                         size_t* entry_idx) const {
  CHECK_GT(numEntries(), size_t(0));
  // Skip num_entries field (4 bytes) at MetaNode head
  size_t byte_offset = sizeof(uint32_t);
  uint64_t total_num_elements = 0;
  for (size_t i = 0; i < numEntries(); i++) {
    MetaEntry entry(chunk_->data() + byte_offset);
    total_num_elements += entry.numElements();
    if (element_idx < total_num_elements) {
      *entry_idx = i;
      return entry.targetHash();
    }
    size_t entry_len = entry.numBytes();
    byte_offset += entry_len;
  }
  *entry_idx = numEntries();
  return Hash();
}

Hash MetaNode::GetChildHashByEntry(size_t entry_idx) const {
  CHECK_GE(entry_idx, size_t(0));
  CHECK_LT(entry_idx, numEntries());
  MetaEntry me(chunk_->data() + entryOffset(entry_idx));
  return me.targetHash();
}

Hash MetaNode::GetChildHashByKey(const OrderedKey& key,
                                 size_t* entry_idx) const {
  CHECK_GT(numEntries(), size_t(0));
  // Skip num_entries field (4 bytes) at MetaNode head
  size_t byte_offset = sizeof(uint32_t);
  for (size_t i = 0; i < numEntries(); i++) {
    MetaEntry entry(chunk_->data() + byte_offset);
    if (key <= entry.orderedKey()) {
      *entry_idx = i;
      return entry.targetHash();
    }
    size_t entry_len = entry.numBytes();
    byte_offset += entry_len;
  }
  *entry_idx = numEntries();
  return Hash();
}


std::unique_ptr<const Segment> MetaNode::GetSegment(size_t start,
    size_t num_elements) const {
  CHECK_LT(start, numEntries());
  if (num_elements == 0) {
    // return an empty segment
    std::unique_ptr<const Segment> seg(
        new VarSegment(data(start)));
    return seg;
  }

  CHECK_LE(start + num_elements, numEntries());

  std::vector<size_t> offsets;

  size_t num_bytes = 0;

  for (size_t i = start; i < start + num_elements; ++i) {
    offsets.push_back(num_bytes);
    num_bytes += len(i);
  }

  std::unique_ptr<const Segment> seg(
      new VarSegment(data(start), num_bytes, std::move(offsets)));
  return seg;
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
