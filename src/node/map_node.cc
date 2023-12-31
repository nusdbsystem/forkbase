// Copyright (c) 2017 The Ustore Authors.

#include "node/map_node.h"

#include <cstring>  // for memcpy
#include "hash/hash.h"
#include "utils/logging.h"
#include "utils/debug.h"

namespace ustore {

ChunkInfo MapChunker::Make(const std::vector<const Segment*>& segments) const {
  // Caculate the total number of entries and bytes for all segments
  size_t num_entries = 0;
  size_t chunk_num_bytes = sizeof(uint32_t);
  for (const auto& seg : segments) {
    CHECK(!seg->empty());
    num_entries += seg->numEntries();
    chunk_num_bytes += seg->numBytes();
  }

  Chunk chunk(ChunkType::kMap, chunk_num_bytes);
  uint32_t unum_entries = static_cast<uint32_t>(num_entries);
  std::memcpy(chunk.m_data(), &unum_entries, sizeof(uint32_t));
  size_t seg_offset = sizeof(uint32_t);
  OrderedKey preKey;
  bool firstKey = true;

  size_t seg_idx = 0;
  for (const auto& seg : segments) {
  // Ensure the key is in strictly increasing order
    for (size_t idx = 0; idx < seg->numEntries(); idx++) {
      const OrderedKey currKey = MapNode::orderedKey(seg->entry(idx));
      // DLOG(INFO) << "Current OrderedKey: " << currKey.ToSlice().ToString()
      //           << " At Segment " << seg_idx
      //           << " Entry Idx " << idx;
      if (firstKey) {
        firstKey = false;
      } else {
        CHECK(preKey < currKey) << " Fail at "
           << "Segment " << seg_idx
           << " Entry Idx: " << idx;
      }
      preKey = currKey;
    }  // end for
    seg->AppendForChunk(chunk.m_data() + seg_offset);
    seg_offset += seg->numBytes();
    ++seg_idx;
  }

  size_t me_num_bytes;
  std::unique_ptr<const byte_t[]> meta_data(MetaEntry::Encode(
      1, num_entries, chunk.hash(), preKey, &me_num_bytes));
  std::unique_ptr<const Segment> meta_seg(
      new VarSegment(std::move(meta_data), me_num_bytes, {0}));
  return {std::move(chunk), std::move(meta_seg)};
}

const byte_t* MapNode::key(const byte_t* entry, size_t* key_size) {
  // Skip the first 4 bytes for item size
  size_t key_size_offset = sizeof(uint32_t);
  *key_size = static_cast<size_t>(*reinterpret_cast<const uint32_t*>
                                  (entry + key_size_offset));
  // Skip the next 4 bytes for key size
  size_t key_offset = key_size_offset + sizeof(uint32_t);
  return entry + key_offset;
}

const byte_t* MapNode::value(const byte_t* entry, size_t* value_size) {
  uint32_t entry_size = *reinterpret_cast<const uint32_t*>(entry);

  // Skip the first 4 bytes for item size
  size_t key_size_offset = sizeof(uint32_t);
  uint32_t key_size = *reinterpret_cast<const uint32_t*>(entry
                                                         + key_size_offset);
  // Skip the next 4 bytes for key size
  size_t value_offset = key_size_offset + sizeof(uint32_t) + key_size;

  *value_size = entry_size - value_offset;
  return entry + value_offset;
}

const KVItem MapNode::kvitem(const byte_t* entry, size_t* item_num_bytes) {
  size_t key_num_bytes;
  size_t val_num_bytes;
  const byte_t* key_data = MapNode::key(entry, &key_num_bytes);
  const byte_t* val_data = MapNode::value(entry, &val_num_bytes);

  //  4 bytes for item size, 4 bytes for key size
  *item_num_bytes = sizeof(uint32_t) + sizeof(uint32_t) + key_num_bytes
                    + val_num_bytes;
  return {{key_data, key_num_bytes}, {val_data, val_num_bytes}};
}

size_t MapNode::Encode(byte_t* buffer, const KVItem& kv_item) {
  const size_t num_byte_offset = 0;
  const size_t key_byte_offset = num_byte_offset + sizeof(uint32_t);
  const size_t key_offset = key_byte_offset + sizeof(uint32_t);
  const size_t val_offset = key_offset + kv_item.key.len();

  const size_t item_num_bytes = MapNode::EncodeNumBytes(kv_item);
  uint32_t uitem_num_bytes = static_cast<uint32_t>(item_num_bytes);
  uint32_t ukey_num_bytes = static_cast<uint32_t>(kv_item.key.len());

  std::memcpy(buffer + num_byte_offset, &uitem_num_bytes, sizeof(uint32_t));
  std::memcpy(buffer + key_byte_offset, &ukey_num_bytes, sizeof(uint32_t));
  std::memcpy(buffer + key_offset, kv_item.key.data(), kv_item.key.len());
  std::memcpy(buffer + val_offset, kv_item.val.data(), kv_item.val.len());
  return item_num_bytes;
}

size_t MapNode::EncodeNumBytes(const KVItem& kv_item) {
  //  4 bytes for entry length, 4 bytes for key size
  return sizeof(uint32_t) + sizeof(uint32_t) + kv_item.key.len()
         + kv_item.val.len();
}

std::unique_ptr<const Segment> MapNode::Encode(
    const std::vector<KVItem>& items) {
  CHECK_GT(items.size(), size_t(0));
  // Calcuate into number of bytes required
  // Meanwhile check key is in strict increasing order
  OrderedKey preKey(false, items[0].key.data(), items[0].key.len());
  size_t total_num_bytes = MapNode::EncodeNumBytes(items[0]);

  for (size_t i = 1; i < items.size(); ++i) {
    OrderedKey currKey(false, items[i].key.data(), items[i].key.len());
    total_num_bytes += MapNode::EncodeNumBytes(items[i]);
    CHECK(preKey < currKey);
    preKey = currKey;
  }

  byte_t* buffer = new byte_t[total_num_bytes];
  // Concat multiple kvitems into a segment
  size_t offset = 0;
  std::vector<size_t> offsets;
  for (const auto& kv_item : items) {
    offsets.push_back(offset);
    offset += MapNode::Encode(buffer + offset, kv_item);
  }

  std::unique_ptr<const byte_t[]> udata(buffer);
  std::unique_ptr<const Segment> seg(
      new VarSegment(std::move(udata), offset, std::move(offsets)));
  return seg;
}


const byte_t* MapNode::data(size_t idx) const {
  CHECK_LT(idx, numEntries());
  size_t offset = offsets_[idx];
  return chunk_->data() + offset;
}

size_t MapNode::len(size_t idx) const {
  DCHECK_LT(idx, numEntries());
  size_t preOffset = 0;
  if (idx == numEntries() - 1) {
    preOffset = chunk_->capacity();
  } else {
    preOffset = offsets_[idx + 1];
  }
  return preOffset - offsets_[idx];
}

OrderedKey MapNode::key(size_t idx) const {
  return MapNode::orderedKey(data(idx));
}

uint64_t MapNode::FindIndexForKey(const OrderedKey& key,
                                  ChunkLoader* loader) const {
  size_t idx = 0;
  for (const size_t offset : offsets_) {
    size_t keyNumBytes = 0;
    const byte_t* keyData = MapNode::key(chunk_->data() + offset, &keyNumBytes);
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

size_t MapNode::numEntries() const {
  return static_cast<size_t>(
             *reinterpret_cast<const uint32_t*>(chunk_->data()));
}

void MapNode::PrecomputeOffsets() {
  CHECK(offsets_.empty());
  // iterate all KVItems in MapNode and accumulate offset
  // Skip num_entries field (4 bytes) at MapNode head
  size_t byte_offset = sizeof(uint32_t);
  OrderedKey preKey;
  for (size_t i = 0; i < numEntries(); i++) {
    offsets_.push_back(byte_offset);
    size_t item_num_bytes;
    MapNode::kvitem(chunk_->data() + byte_offset, &item_num_bytes);

    // Check in strict increasing order key
    OrderedKey currKey = MapNode::orderedKey(chunk_->data() + byte_offset);
    if (i > 0) {CHECK(preKey < currKey); }
    preKey = currKey;
    byte_offset += item_num_bytes;
  }
}
size_t MapNode::Copy(size_t start, size_t num_bytes, byte_t* buffer) const {
  LOG(FATAL) << "Not Supported Yet";
  return 0;
}
size_t MapNode::GetLength(size_t start, size_t end) const {
  LOG(FATAL) << "Not Supported Yet";
  return 0;
}

std::unique_ptr<const Segment> MapNode::GetSegment(
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
    size_t key_len, val_len;
    const byte_t* key_data = MapNode::key(seg->entry(i), &key_len);
    Slice key(key_data, key_len);

    const byte_t* val_data = MapNode::value(seg->entry(i), &val_len);
    Slice val(val_data, val_len);

    // DLOG(INFO) << "Key: " << key.ToString()
    //            << " Val: " << val.ToString();
  }


#endif
  return seg;
}
}  // namespace ustore
