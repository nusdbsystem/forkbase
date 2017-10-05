// Copyright (c) 2017 The Ustore Authors.

#include "node/set_node.h"

#include <cstring>  // for memcpy
#include "hash/hash.h"
#include "node/orderedkey.h"
#include "utils/logging.h"
#include "utils/debug.h"

namespace ustore {

ChunkInfo SetChunker::Make(const std::vector<const Segment*>& segments) const {
  // Caculate the total number of entries and bytes for all segments
  size_t num_entries = 0;
  size_t chunk_num_bytes = sizeof(uint32_t);
  for (const auto& seg : segments) {
    CHECK(!seg->empty());
    num_entries += seg->numEntries();
    chunk_num_bytes += seg->numBytes();
  }

  Chunk chunk(ChunkType::kSet, chunk_num_bytes);
  uint32_t unum_entries = static_cast<uint32_t>(num_entries);
  std::memcpy(chunk.m_data(), &unum_entries, sizeof(uint32_t));
  size_t seg_offset = sizeof(uint32_t);
  OrderedKey preKey;
  bool firstKey = true;

  for (const auto& seg : segments) {
  // Ensure the key is in strictly increasing order
    for (size_t idx = 0; idx < seg->numEntries(); idx++) {
      const OrderedKey currKey = SetNode::orderedKey(seg->entry(idx));
      if (firstKey) {
        firstKey = false;
      } else {
        CHECK(preKey < currKey);
      }
      preKey = currKey;
    }  // end for
    seg->AppendForChunk(chunk.m_data() + seg_offset);
    seg_offset += seg->numBytes();
  }

  size_t me_num_bytes;
  std::unique_ptr<const byte_t[]> meta_data(MetaEntry::Encode(
      1, num_entries, chunk.hash(), preKey, &me_num_bytes));
  std::unique_ptr<const Segment> meta_seg(
      new VarSegment(std::move(meta_data), me_num_bytes, {0}));
  return {std::move(chunk), std::move(meta_seg)};
}

const byte_t* SetNode::key(const byte_t* entry, size_t* key_size) {
  size_t key_offset = sizeof(uint32_t);
  *key_size = static_cast<size_t>(*reinterpret_cast<const uint32_t*>
                                  (entry)) - key_offset;
  // Skip the first 4 bytes for key size
  return entry + key_offset;
}

const Slice SetNode::item(const byte_t* entry, size_t* item_num_bytes) {
  size_t key_num_bytes;
  const byte_t* key_data = SetNode::key(entry, &key_num_bytes);

  //  4 bytes for key size
  *item_num_bytes = sizeof(uint32_t) + key_num_bytes;
  return {key_data, key_num_bytes};
}

size_t SetNode::Encode(byte_t* buffer, const Slice& item) {
  const size_t key_byte_offset = 0;
  const size_t key_offset = key_byte_offset + sizeof(uint32_t);

  const size_t item_num_bytes = SetNode::EncodeNumBytes(item);
  uint32_t uitem_num_bytes = static_cast<uint32_t>(item_num_bytes);

  std::memcpy(buffer + key_byte_offset, &uitem_num_bytes, sizeof(uint32_t));
  std::memcpy(buffer + key_offset, item.data(), item.len());
  return item_num_bytes;
}

size_t SetNode::EncodeNumBytes(const Slice& item) {
  //  4 bytes for key size
  return sizeof(uint32_t) + item.len();
}

std::unique_ptr<const Segment> SetNode::Encode(
    const std::vector<Slice>& items) {
  CHECK_GT(items.size(), size_t(0));
  // Calcuate into number of bytes required
  // Meanwhile check key is in strict increasing order
  OrderedKey preKey(false, items[0].data(), items[0].len());
  size_t total_num_bytes = SetNode::EncodeNumBytes(items[0]);

  for (size_t i = 1; i < items.size(); ++i) {
    OrderedKey currKey(false, items[i].data(), items[i].len());
    total_num_bytes += SetNode::EncodeNumBytes(items[i]);
    CHECK(preKey < currKey);
    preKey = currKey;
  }

  byte_t* buffer = new byte_t[total_num_bytes];
  // Concat multiple Slices into a segment
  size_t offset = 0;
  std::vector<size_t> offsets;
  for (const auto& item : items) {
    offsets.push_back(offset);
    offset += SetNode::Encode(buffer + offset, item);
  }

  std::unique_ptr<const byte_t[]> udata(buffer);
  std::unique_ptr<const Segment> seg(
      new VarSegment(std::move(udata), offset, std::move(offsets)));
  return seg;
}


const byte_t* SetNode::data(size_t idx) const {
  CHECK_LT(idx, numEntries());
  size_t offset = offsets_[idx];
  return chunk_->data() + offset;
}

size_t SetNode::len(size_t idx) const {
  DCHECK_LT(idx, numEntries());
  size_t preOffset = 0;
  if (idx == numEntries() - 1) {
    preOffset = chunk_->capacity();
  } else {
    preOffset = offsets_[idx + 1];
  }
  return preOffset - offsets_[idx];
}

OrderedKey SetNode::key(size_t idx) const {
  return SetNode::orderedKey(data(idx));
}

uint64_t SetNode::FindIndexForKey(const OrderedKey& key,
                                  ChunkLoader* loader) const {
  size_t idx = 0;
  for (const size_t offset : offsets_) {
    size_t keyNumBytes = 0;
    const byte_t* keyData = SetNode::key(chunk_->data() + offset, &keyNumBytes);
    OrderedKey currKey(false, keyData, keyNumBytes);
    if (currKey == key) {
      return idx;
    } else if (currKey > key) {
      return idx;
    }
    ++idx;
  }
  return idx;
}

size_t SetNode::numEntries() const {
  return static_cast<size_t>(
             *reinterpret_cast<const uint32_t*>(chunk_->data()));
}

void SetNode::PrecomputeOffsets() {
  CHECK(offsets_.empty());
  // iterate all Slices in SetNode and accumulate offset
  // Skip num_entries field (4 bytes) at SetNode head
  size_t byte_offset = sizeof(uint32_t);
  OrderedKey preKey;
  for (size_t i = 0; i < numEntries(); i++) {
    offsets_.push_back(byte_offset);
    size_t item_num_bytes;
    SetNode::item(chunk_->data() + byte_offset, &item_num_bytes);

    // Check in strict increasing order key
    OrderedKey currKey = SetNode::orderedKey(chunk_->data() + byte_offset);
    if (i > 0) {CHECK(preKey < currKey); }
    preKey = currKey;
    byte_offset += item_num_bytes;
  }
}
size_t SetNode::Copy(size_t start, size_t num_bytes, byte_t* buffer) const {
  LOG(FATAL) << "Not Supported Yet";
  return 0;
}
size_t SetNode::GetLength(size_t start, size_t end) const {
  LOG(FATAL) << "Not Supported Yet";
  return 0;
}
std::unique_ptr<const Segment> SetNode::GetSegment(
    size_t start, size_t num_elements) const {
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
#ifdef DEBUG
  DLOG(INFO) << "Get Segment from " << start
             << " for # elements " << num_elements;

  for (size_t i = 0; i < seg->numEntries(); ++i) {
    size_t key_len;
    const byte_t* key_data = SetNode::key(seg->entry(i), &key_len);
    Slice key(key_data, key_len);

    // DLOG(INFO) << "Key: " << key.ToString();
  }
#endif
  return seg;
}
}  // namespace ustore
