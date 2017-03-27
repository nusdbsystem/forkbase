// Copyright (c) 2017 The Ustore Authors.

#include "node/node.h"

#include <cstring>  // for memcpy
#include "hash/hash.h"
#include "node/blob_node.h"
#include "node/orderedkey.h"
#include "utils/logging.h"

namespace ustore {

const ChunkInfo MetaNode::MakeChunk(
    const std::vector<const byte_t*>& entries_data,
    const std::vector<size_t>& entries_num_bytes) {
  // both vectors have the number of elements
  CHECK_EQ(entries_data.size(), entries_num_bytes.size());
  uint32_t num_entries = entries_data.size();
  // compute number of bytes for this new chunk
  // the first four bytes to encode num_entry
  size_t chunk_num_bytes = sizeof(uint32_t);
  for (const size_t entry_num_byte : entries_num_bytes) {
    chunk_num_bytes += entry_num_byte;
  }

  Chunk* chunk = new Chunk(ustore::kMetaChunk, chunk_num_bytes);
  // encode num_entries
  memcpy(chunk->m_data(), &num_entries, sizeof(uint32_t));

  size_t entry_offset = sizeof(uint32_t);
  // repeatedly copy bytes from metaentry one by one
  uint32_t total_num_leaves = 0;
  uint32_t total_num_elements = 0;
  const MetaEntry* pre_me = nullptr;
  for (size_t idx = 0; idx < num_entries; idx++) {
    memcpy(chunk->m_data() + entry_offset, entries_data[idx],
           entries_num_bytes[idx]);
    entry_offset += entries_num_bytes[idx];
    const MetaEntry* me = new MetaEntry(entries_data[idx]);
    if (idx > 0) {
      CHECK(pre_me->orderedKey() <= me->orderedKey())
        << "MetaEntry shall be in non-descending order.";
    }
    total_num_elements += me->numElements();
    total_num_leaves += me->numLeaves();
    delete pre_me;
    pre_me = me;
  }

  const OrderedKey key = pre_me->orderedKey();
  delete pre_me;
  size_t me_num_bytes;
  const byte_t* me_data =
    MetaEntry::Encode(total_num_leaves, total_num_elements, chunk->hash(), key,
                      &me_num_bytes);
  return {chunk, {me_data, me_num_bytes}};
}

size_t MetaNode::numEntries() const {
  // read the first 4 bytes of chunk data as a uint32_t
  return *reinterpret_cast<const uint32_t*>(chunk_->data());
}

uint64_t MetaNode::numElements() const {
  return numElementsUntilEntry(numEntries());
}

uint64_t MetaNode::numElementsUntilEntry(size_t entry_idx) const {
  CHECK_GE(entry_idx, 0);
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
  return chunk_->m_data() + byte_offset;
}

size_t MetaNode::len(size_t idx) const {
  CHECK_GE(idx, 0);
  CHECK_LT(idx, numEntries());
  // iterate all MetaEntries in MetaNode and accumulate offset
  // Skip num_entries field (4 bytes) at MetaNode head
  size_t byte_offset = entryOffset(idx);
  MetaEntry entry(chunk_->data() + byte_offset);
  return entry.numBytes();
}

uint64_t MetaNode::entryOffset(size_t idx) const {
  // make sure 0 <= idx < numElements()
  CHECK_GE(idx, 0);
  CHECK_LT(idx, numEntries());
  // iterate all MetaEntries in MetaNode and accumulate offset
  // Skip num_entries field (4 bytes) at MetaNode head
  size_t byte_offset = sizeof(uint32_t);
  // TODO(pingcheng): scan might affect performance, consider later
  for (size_t i = 0; i < idx; i++) {
    MetaEntry entry(chunk_->data() + byte_offset);
    size_t entry_len = entry.numBytes();
    byte_offset += entry_len;
  }
  return byte_offset;
}

const Hash MetaNode::GetChildHashByIndex(size_t element_idx,
                                         size_t* entry_idx) const {
  CHECK_GT(numEntries(), 0);
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

const Hash MetaNode::GetChildHashByEntry(size_t entry_idx) const {
  CHECK_GE(entry_idx, 0);
  CHECK_LT(entry_idx, numEntries());
  MetaEntry me(chunk_->data() + entryOffset(entry_idx));
  return me.targetHash();
}

const Hash MetaNode::GetChildHashByKey(const OrderedKey& key,
                                       size_t* entry_idx) const {
  CHECK_GT(numEntries(), 0);
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

const byte_t* MetaEntry::Encode(uint32_t num_leaves, uint64_t num_elements,
                                const Hash& data_hash, const OrderedKey& key,
                                size_t* encode_len) {
  uint32_t num_bytes =  KEY_OFFSET + key.numBytes();
  byte_t* data = new byte_t[num_bytes];
  memcpy(data + NUM_BYTE_OFFSET, &num_bytes, sizeof(uint32_t));
  memcpy(data + NUM_LEAF_OFFSET, &num_leaves, sizeof(uint32_t));
  memcpy(data + NUM_ELEMENT_OFFSET, &num_elements, sizeof(uint64_t));
  memcpy(data + HASH_OFFSET, data_hash.value(), HASH_BYTE_LEN);
  key.encode(data + KEY_OFFSET);
  *encode_len = num_bytes;
  return data;
}

const OrderedKey MetaEntry::orderedKey() const {
  // remaining bytes of MetaEntry are for ordered key
  size_t key_num_bytes = numBytes() - KEY_OFFSET;
  return OrderedKey(data_ + KEY_OFFSET, key_num_bytes);
}

size_t MetaEntry::numBytes() const {
  return *(reinterpret_cast<const uint32_t*>(data_ + NUM_BYTE_OFFSET));
}

uint32_t MetaEntry::numLeaves() const {
  return *(reinterpret_cast<const uint32_t*>(data_ + NUM_LEAF_OFFSET));
}

uint64_t MetaEntry::numElements() const {
  return *(reinterpret_cast<const uint64_t*>(data_ + NUM_ELEMENT_OFFSET));
}

const Hash MetaEntry::targetHash() const { return Hash(data_ + HASH_OFFSET); }

}  // namespace ustore
