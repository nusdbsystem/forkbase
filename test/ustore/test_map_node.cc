// Copyright (c) 2017 The Ustore Authors.

#include <cstddef>
#include <cstring>
#include <string>

#include "gtest/gtest.h"

#include "node/map_node.h"

TEST(MapNode, Codec) {
  constexpr const ustore::byte_t key_data[] = "key";
  constexpr const ustore::byte_t val_data[] = "value";
  size_t key_num_bytes = 3;
  size_t val_num_bytes = 5;
  const ustore::OrderedKey expectedKey(false, key_data, key_num_bytes);

  ustore::KVItem item {{key_data, key_num_bytes}, {val_data, val_num_bytes}};
  ustore::byte_t* buffer = new ustore::byte_t[100];

// Test for Encoding Scheme
  size_t itemNumBytes = ustore::MapNode::Encode(buffer, item);

  EXPECT_EQ(key_num_bytes + val_num_bytes + 2 * sizeof(uint32_t),
            itemNumBytes);

  EXPECT_EQ(itemNumBytes,
            static_cast<size_t>(*reinterpret_cast<uint32_t*>(buffer)));

  size_t key_num_byte_offset = sizeof(uint32_t);
  EXPECT_EQ(key_num_bytes,
            static_cast<size_t>(*reinterpret_cast<uint32_t*>(
                                  buffer + key_num_byte_offset)));

  size_t key_offset = key_num_byte_offset + sizeof(uint32_t);
  EXPECT_EQ(0, memcmp(key_data, buffer + key_offset, key_num_bytes));

  size_t val_offset = key_offset + key_num_bytes;
  EXPECT_EQ(0, memcmp(val_data, buffer + val_offset, val_num_bytes));

// Test for three decoders
  EXPECT_EQ(expectedKey, ustore::MapNode::orderedKey(buffer));

  size_t actual_key_num_bytes;
  const ustore::byte_t* actual_key_data =
      ustore::MapNode::key(buffer, &actual_key_num_bytes);
  EXPECT_EQ(key_num_bytes, actual_key_num_bytes);
  EXPECT_EQ(0,
            memcmp(key_data, actual_key_data, key_num_bytes));

  size_t actual_val_num_bytes;
  const ustore::byte_t* actual_val_data =
      ustore::MapNode::value(buffer, &actual_val_num_bytes);
  EXPECT_EQ(val_num_bytes, actual_val_num_bytes);
  EXPECT_EQ(0,
            memcmp(val_data, actual_val_data, val_num_bytes));

  size_t actual_item_num_bytes;
  ustore::KVItem kvitem =
      ustore::MapNode::kvitem(buffer, &actual_item_num_bytes);
  EXPECT_EQ(key_num_bytes, kvitem.key.len());
  EXPECT_EQ(val_num_bytes, kvitem.val.len());
  EXPECT_EQ(0,
            memcmp(key_data, kvitem.key.data(), key_num_bytes));
  EXPECT_EQ(0,
            memcmp(val_data, kvitem.val.data(), val_num_bytes));
  delete[] buffer;
}

TEST(MapNode, Basic) {
  // Create two segments,
  //   the first with 2 items
  //   the second with 1 items
  constexpr const ustore::byte_t k1[] = "k1";
  constexpr const ustore::byte_t v1[] = "v1";
  constexpr const ustore::byte_t k2[] = "k22";
  constexpr const ustore::byte_t v2[] = "v22";
  constexpr const ustore::byte_t k3[] = "k333";
  constexpr const ustore::byte_t v3[] = "v333";

  ustore::KVItem kv1{{k1, 2}, {v1, 2}};
  ustore::KVItem kv2{{k2, 3}, {v2, 3}};
  ustore::KVItem kv3{{k3, 4}, {v3, 4}};

  std::unique_ptr<const ustore::Segment> seg12 =
      ustore::MapNode::Encode({kv1, kv2});
  std::unique_ptr<const ustore::Segment> seg3 =
      ustore::MapNode::Encode({kv3});

  std::vector<const ustore::Segment*> segs {seg12.get(), seg3.get()};

  ustore::ChunkInfo chunk_info = ustore::MapChunker::Instance()->Make(segs);

  // First 4 bytes of chunk encode number of items
  EXPECT_EQ(3, *reinterpret_cast<const uint32_t*>(chunk_info.chunk.data()));
  // Subsquent chunk data shall concat 2 kv items
  EXPECT_EQ(0,
            memcmp(chunk_info.chunk.data() + sizeof(uint32_t),
                   seg12->data(),
                   seg12->numBytes()));
  EXPECT_EQ(0,
            memcmp(chunk_info.chunk.data() + sizeof(uint32_t)
                                            + seg12->numBytes(),
                   seg3->data(),
                   seg3->numBytes()));

  // Test on the created metaentry
  ASSERT_EQ(1, chunk_info.meta_seg->numEntries());
  const ustore::byte_t* me_data = chunk_info.meta_seg->entry(0);
  ustore::MetaEntry me(me_data);

  EXPECT_EQ(chunk_info.chunk.hash(), me.targetHash());
  EXPECT_EQ(chunk_info.meta_seg->numBytes(), me.numBytes());

  const ustore::OrderedKey me_key = me.orderedKey();
  ustore::OrderedKey key3(false, k3, 4);
  EXPECT_EQ(key3, me_key);

  EXPECT_EQ(3, me.numElements());
  EXPECT_EQ(1, me.numLeaves());

  // Test on MetaNode
  ustore::MapNode mnode(&chunk_info.chunk);
  ASSERT_EQ(3, mnode.numEntries());
  EXPECT_EQ(ustore::MapNode::EncodeNumBytes(kv1), mnode.len(0));
  EXPECT_EQ(ustore::MapNode::EncodeNumBytes(kv3), mnode.len(2));

  const ustore::byte_t* kv2_data = mnode.data(1);
  size_t item2_num_bytes;
  const ustore::KVItem item2 = ustore::MapNode::kvitem(kv2_data,
                                                       &item2_num_bytes);
  EXPECT_EQ(3, item2.key.len());
  EXPECT_EQ(3, item2.val.len());
  EXPECT_EQ(0, memcmp(item2.key.data(), k2, 3));
  EXPECT_EQ(0, memcmp(item2.val.data(), v2, 3));

  // Find the exact key
  ustore::OrderedKey key1(false, k1, 2);
  EXPECT_EQ(0, mnode.GetIdxForKey(key1));

  // Find not the exact key
  constexpr ustore::byte_t k12[] = "k12";
  ustore::OrderedKey key2(false, k12, 3);
  EXPECT_EQ(1, mnode.GetIdxForKey(key2));

  // Search to the end
  constexpr ustore::byte_t k4[] = "k4";
  ustore::OrderedKey key4(false, k4, 2);
  EXPECT_EQ(3, mnode.GetIdxForKey(key4));
}
