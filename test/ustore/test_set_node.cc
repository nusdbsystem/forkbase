// Copyright (c) 2017 The Ustore Authors.

#include <cstddef>
#include <cstring>
#include <string>

#include "gtest/gtest.h"

#include "node/set_node.h"

TEST(SetNode, Codec) {
  constexpr const ustore::byte_t key_data[] = "key";
  size_t key_num_bytes = 3;
  const ustore::OrderedKey expectedKey(false, key_data, key_num_bytes);

  ustore::Slice item {key_data, key_num_bytes};
  ustore::byte_t* buffer = new ustore::byte_t[100];

// Test for Encoding Scheme
  size_t itemNumBytes = ustore::SetNode::Encode(buffer, item);

  EXPECT_EQ(key_num_bytes + sizeof(uint32_t), itemNumBytes);

  EXPECT_EQ(itemNumBytes,
            static_cast<size_t>(*reinterpret_cast<uint32_t*>(buffer)));

  size_t key_num_byte_offset = sizeof(uint32_t);

  size_t key_offset = key_num_byte_offset;
  EXPECT_EQ(0, memcmp(key_data, buffer + key_offset, key_num_bytes));

// Test for decoder
  EXPECT_EQ(expectedKey, ustore::SetNode::orderedKey(buffer));

  size_t actual_key_num_bytes;
  const ustore::byte_t* actual_key_data =
      ustore::SetNode::key(buffer, &actual_key_num_bytes);
  EXPECT_EQ(key_num_bytes, actual_key_num_bytes);
  EXPECT_EQ(0,
            memcmp(key_data, actual_key_data, key_num_bytes));

  size_t actual_item_num_bytes;
  ustore::Slice item0 =
      ustore::SetNode::item(buffer, &actual_item_num_bytes);
  EXPECT_EQ(key_num_bytes, item0.len());
  EXPECT_EQ(0,
            memcmp(key_data, item0.data(), key_num_bytes));
  delete[] buffer;
}

TEST(SetNode, Basic) {
  // Create two segments,
  //   the first with 2 items
  //   the second with 1 items
  constexpr const ustore::byte_t k1[] = "k1";
  constexpr const ustore::byte_t k2[] = "k22";
  constexpr const ustore::byte_t k3[] = "k333";

  ustore::Slice kv1{k1, 2};
  ustore::Slice kv2{k2, 3};
  ustore::Slice kv3{k3, 4};

  std::unique_ptr<const ustore::Segment> seg12 =
      ustore::SetNode::Encode({kv1, kv2});
  std::unique_ptr<const ustore::Segment> seg3 =
      ustore::SetNode::Encode({kv3});

  std::vector<const ustore::Segment*> segs {seg12.get(), seg3.get()};

  ustore::ChunkInfo chunk_info = ustore::SetChunker::Instance()->Make(segs);

  // First 4 bytes of chunk encode number of items
  EXPECT_EQ(uint32_t(3),
            *reinterpret_cast<const uint32_t*>(chunk_info.chunk.data()));
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
  ASSERT_EQ(uint32_t(1), chunk_info.meta_seg->numEntries());
  const ustore::byte_t* me_data = chunk_info.meta_seg->entry(0);
  ustore::MetaEntry me(me_data);

  EXPECT_EQ(chunk_info.chunk.hash(), me.targetHash());
  EXPECT_EQ(chunk_info.meta_seg->numBytes(), me.numBytes());

  const ustore::OrderedKey me_key = me.orderedKey();
  ustore::OrderedKey key3(false, k3, 4);
  EXPECT_EQ(key3, me_key);

  EXPECT_EQ(size_t(3), me.numElements());
  EXPECT_EQ(size_t(1), me.numLeaves());

  // Test on MetaNode
  ustore::SetNode mnode(&chunk_info.chunk);
  ASSERT_EQ(size_t(3), mnode.numEntries());
  EXPECT_EQ(ustore::SetNode::EncodeNumBytes(kv1), mnode.len(0));
  EXPECT_EQ(ustore::SetNode::EncodeNumBytes(kv3), mnode.len(2));

  const ustore::byte_t* kv2_data = mnode.data(1);
  size_t item2_num_bytes;
  const ustore::Slice item2 = ustore::SetNode::item(kv2_data,
                                                       &item2_num_bytes);
  EXPECT_EQ(size_t(3), item2.len());
  EXPECT_EQ(0, memcmp(item2.data(), k2, 3));

  // Find the exact key
  ustore::OrderedKey key1(false, k1, 2);
  EXPECT_EQ(size_t(0), mnode.FindIndexForKey(key1, nullptr));

  // Find not the exact key
  constexpr ustore::byte_t k12[] = "k12";
  ustore::OrderedKey key2(false, k12, 3);
  EXPECT_EQ(size_t(1), mnode.FindIndexForKey(key2, nullptr));

  // Search to the end
  constexpr ustore::byte_t k4[] = "k4";
  ustore::OrderedKey key4(false, k4, 2);
  EXPECT_EQ(size_t(3), mnode.FindIndexForKey(key4, nullptr));

  auto seg = mnode.GetSegment(1, 2);
  EXPECT_EQ(size_t(2), seg->numEntries());

  size_t s;
  const ustore::Slice actual_k2 =
      ustore::SetNode::item(seg->entry(0), &s);
  EXPECT_EQ(kv2.len(), actual_k2.len());
  EXPECT_EQ(0,
            std::memcmp(kv2.data(),
                        actual_k2.data(),
                        kv2.len()));

  const ustore::Slice actual_k3 =
      ustore::SetNode::item(seg->entry(1), &s);
  EXPECT_EQ(kv3.len(), actual_k3.len());
  EXPECT_EQ(0,
            std::memcmp(kv3.data(),
                        actual_k3.data(),
                        kv3.len()));
}
