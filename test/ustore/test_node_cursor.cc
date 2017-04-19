// Copyright (c) 2017 The Ustore Authors.
#include <algorithm>
#include <cstring>
#include <string>
#include "gtest/gtest.h"

#include "chunk/chunker.h"
#include "node/cursor.h"
#include "node/map_node.h"
#include "types/umap.h"
#include "store/chunk_loader.h"

TEST(NodeCursor, SingleNode) {
  // Construct a tree with only a root blob node
  const ustore::byte_t ra[] = "abc";

  size_t ra_num_bytes = sizeof(ra) - 1;  // excluding trailing \0
  ustore::Chunk ca(ustore::ChunkType::kBlob, ra_num_bytes);
  std::copy(ra, ra + ra_num_bytes, ca.m_data());

  ustore::ChunkStore* chunk_store = ustore::store::GetChunkStore();
  // Write the constructed chunk to storage
  EXPECT_TRUE(chunk_store->Put(ca.hash(), ca));

  ustore::ChunkLoader loader;
  ustore::NodeCursor* cr =
      ustore::NodeCursor::GetCursorByIndex(ca.hash(), 1, &loader);

  ASSERT_EQ('b', *(cr->current()));
  EXPECT_EQ(1, cr->numCurrentBytes());

  EXPECT_TRUE(cr->Retreat(false));  // point the 0th element

  EXPECT_FALSE(cr->Retreat(false));  // point to seq head
  EXPECT_FALSE(cr->Retreat(false));  // still point to seq head
  EXPECT_FALSE(cr->Retreat(true));   // still point to seq head

  EXPECT_EQ(nullptr, cr->current());
  EXPECT_EQ(0, cr->numCurrentBytes());
  EXPECT_TRUE(cr->isBegin());

  EXPECT_TRUE(cr->Advance(false));  // point the 0th element
  ASSERT_EQ('a', *(cr->current()));

  EXPECT_TRUE(cr->Advance(false));  // point to 1st element
  ASSERT_EQ('b', *(cr->current()));
  EXPECT_TRUE(cr->Advance(false));  // point to 2nd element
  ASSERT_EQ('c', *(cr->current()));
  auto last_entry = cr->current();

  EXPECT_FALSE(cr->Advance(false));  // point to seq end
  EXPECT_FALSE(cr->Advance(false));  // point to seq end

  EXPECT_EQ(last_entry + 1, cr->current());
  EXPECT_TRUE(cr->isEnd());

  cr->seek(1);
  EXPECT_EQ(1, cr->idx());
}

TEST(NodeCursor, Tree) {
  // Construct a tree with two blob nodes as leaves.
  //   first blob node contains "aa"
  //   second blob node contains "bbb"
  const ustore::byte_t ra[] = "ab";

  size_t ra_num_bytes = sizeof(ra) - 1;  // excluding trailing \0
  ustore::Chunk ca(ustore::ChunkType::kBlob, ra_num_bytes);
  std::copy(ra, ra + ra_num_bytes, ca.m_data());
  ustore::OrderedKey ka(1);

  size_t mta_numBytes = 0;
  const ustore::byte_t* me_a = ustore::MetaEntry::Encode(
      ra_num_bytes, ra_num_bytes, ca.hash(), ka, &mta_numBytes);

  const ustore::byte_t rb[] = "cde";
  size_t rb_num_bytes = sizeof(rb) - 1;  // excluding trailing \0
  ustore::Chunk cb(ustore::ChunkType::kBlob, rb_num_bytes);
  std::copy(rb, rb + rb_num_bytes, cb.m_data());
  ustore::OrderedKey kb(2);

  size_t mtb_numBytes = 0;
  const ustore::byte_t* me_b = ustore::MetaEntry::Encode(
      rb_num_bytes, rb_num_bytes, cb.hash(), kb, &mtb_numBytes);

  ustore::VarSegment seg_a(me_a, mta_numBytes, {0});
  ustore::VarSegment seg_b(me_b, mtb_numBytes, {0});
  // Create MetaNode Chunk
  ustore::ChunkInfo cm_info =
      ustore::MetaChunker::Instance()->make({&seg_a, &seg_b});
  // delete the useless MetaEntry bytes

  const ustore::Chunk* cm = cm_info.chunk.release();

  ustore::ChunkStore* chunk_store = ustore::store::GetChunkStore();

  // Write the constructed chunk to storage
  EXPECT_TRUE(chunk_store->Put(ca.hash(), ca));
  EXPECT_TRUE(chunk_store->Put(cb.hash(), cb));
  EXPECT_TRUE(chunk_store->Put(cm->hash(), *cm));

  ///////////////////////////////////////////////////////////////
  ustore::ChunkLoader loader;

  ustore::NodeCursor* leaf_cursor =
      ustore::NodeCursor::GetCursorByIndex(cm->hash(), 1, &loader);

  ASSERT_NE(nullptr, leaf_cursor);
  EXPECT_EQ(1, leaf_cursor->numCurrentBytes());
  EXPECT_EQ('b', *leaf_cursor->current());

  ustore::NodeCursor* cr_copy = new ustore::NodeCursor(*leaf_cursor);

  //// Test Advancing
  // Advance cursor to reach leaf node end
  EXPECT_FALSE(leaf_cursor->Advance(false));
  // Advance from leaf node end
  EXPECT_FALSE(leaf_cursor->Advance(false));

  // Advance to the next chunk
  EXPECT_TRUE(leaf_cursor->Advance(true));
  EXPECT_EQ('c', *leaf_cursor->current());

  // leaf cursor retreats to seq start
  EXPECT_FALSE(leaf_cursor->Retreat(false));
  EXPECT_TRUE(leaf_cursor->Retreat(true));
  EXPECT_EQ('b', *leaf_cursor->current());

  // Cross the boundary to reach the second blob node
  EXPECT_TRUE(cr_copy->Advance(true));

  // the original cursor does not change
  EXPECT_EQ('b', *leaf_cursor->current());
  EXPECT_EQ('c', *cr_copy->current());
  EXPECT_TRUE(cr_copy->Advance(true));
  EXPECT_TRUE(cr_copy->Advance(true));
  auto last_entry = cr_copy->current();

  // cursor points to Node end after advancing
  EXPECT_FALSE(cr_copy->Advance(true));
  EXPECT_EQ(last_entry + 1, cr_copy->current());
  EXPECT_EQ(0, cr_copy->numCurrentBytes());
  // cursor points to Node end before advancing
  EXPECT_FALSE(cr_copy->Advance(true));

  //// Test Retreating
  ustore::NodeCursor* cur2 =
      ustore::NodeCursor::GetCursorByIndex(cm->hash(), 3, &loader);
  EXPECT_EQ('d', *cur2->current());
  // // points to the frist element after the following retreat
  EXPECT_TRUE(cur2->Retreat(false));
  // // Point to seq start after the following retreat.
  EXPECT_FALSE(cur2->Retreat(false));
  // // Still Point to seq start after the following retreat.
  EXPECT_FALSE(cur2->Retreat(false));

  // // // Cross the boundary and point to the first node
  EXPECT_TRUE(cur2->Retreat(true));
  EXPECT_EQ('b', *cur2->current());

  EXPECT_TRUE(cur2->Retreat(true));
  EXPECT_EQ('a', *cur2->current());

  // // Point to Seq Start
  EXPECT_FALSE(cur2->Retreat(true));
  EXPECT_EQ(nullptr, cur2->current());

  // // last element
  ustore::NodeCursor* cur3 =
      ustore::NodeCursor::GetCursorByIndex(cm->hash(), 4, &loader);
  EXPECT_EQ('e', *cur3->current());

  // test the case when index exceeds num elements
  ustore::NodeCursor* cur4 =
      ustore::NodeCursor::GetCursorByIndex(cm->hash(), 5, &loader);
  EXPECT_TRUE(cur4->isEnd());

  ustore::NodeCursor* cur5 =
      ustore::NodeCursor::GetCursorByIndex(cm->hash(), 6, &loader);
  EXPECT_TRUE(cur5->isEnd());

  delete leaf_cursor;
  delete cr_copy;
  delete cur2;
  delete cur3;
  delete cur4;
  delete cur5;
  delete cm;
}

TEST(NodeCursor, SingleNodeByKey) {
  constexpr const ustore::byte_t k1[] = "k1";
  constexpr const ustore::byte_t v1[] = "v1";
  constexpr const ustore::byte_t k2[] = "k22";
  constexpr const ustore::byte_t v2[] = "v22";
  constexpr const ustore::byte_t k3[] = "k333";
  constexpr const ustore::byte_t v3[] = "v333";

  ustore::KVItem kv1{k1, v1, 2, 2};
  ustore::KVItem kv2{k2, v2, 3, 3};
  ustore::KVItem kv3{k3, v3, 4, 4};

  ustore::byte_t* seg_data12 = new ustore::byte_t[100];
  ustore::byte_t* seg_data3 = new ustore::byte_t[100];

  size_t kv1_num_bytes = ustore::MapNode::encode(seg_data12, kv1);
  size_t kv2_num_bytes = ustore::MapNode::encode(seg_data12
                                                 + kv1_num_bytes, kv2);
  size_t kv3_num_bytes = ustore::MapNode::encode(seg_data3, kv3);

  size_t seg12_num_bytes = kv1_num_bytes + kv2_num_bytes;
  ustore::VarSegment seg12(seg_data12, seg12_num_bytes, {0, kv1_num_bytes});

  size_t seg3_num_bytes = kv3_num_bytes;
  ustore::VarSegment seg3(seg_data3, seg3_num_bytes, {0});

  std::vector<const ustore::Segment*> segs {&seg12, &seg3};
  ustore::ChunkInfo chunk_info = ustore::MapChunker::Instance()->make(segs);

  const ustore::Chunk* chunk = chunk_info.chunk.get();
  ustore::ChunkStore* chunk_store = ustore::store::GetChunkStore();
  ustore::ChunkLoader loader;
  EXPECT_TRUE(chunk_store->Put(chunk->hash(), *chunk));

  // Find the smallest key
  bool found = true;
  constexpr ustore::byte_t k0[] = "k0";
  const ustore::OrderedKey key0(false, k0, 2);
  ustore::NodeCursor* cursor = ustore::NodeCursor::GetCursorByKey(
                                  chunk->hash(), key0,
                                  &loader, &found);
  EXPECT_EQ(0, cursor->idx());
  EXPECT_FALSE(found);
  delete cursor;

  // Find the exact key
  found = false;
  const ustore::OrderedKey key1(false, k1, 2);
  cursor = ustore::NodeCursor::GetCursorByKey(
                                  chunk->hash(), key1,
                                  &loader, &found);
  EXPECT_EQ(0, cursor->idx());
  EXPECT_TRUE(found);
  delete cursor;

  // Find the non-exact key
  found = true;
  constexpr ustore::byte_t k12[] = "k12";
  const ustore::OrderedKey key12(false, k12, 3);
  cursor = ustore::NodeCursor::GetCursorByKey(
                                  chunk->hash(), key12,
                                  &loader, &found);
  EXPECT_EQ(1, cursor->idx());
  EXPECT_FALSE(found);
  delete cursor;

  // Find the non-exact key
  found = true;
  constexpr ustore::byte_t k4[] = "k4";
  const ustore::OrderedKey key4(false, k4, 2);
  cursor = ustore::NodeCursor::GetCursorByKey(
                                  chunk->hash(), key4,
                                  &loader, &found);
  EXPECT_EQ(3, cursor->idx());
  EXPECT_FALSE(found);
  delete cursor;
}

TEST(NodeCursor, TreeByKey) {
  // Construct a tree and access by key
  constexpr const ustore::byte_t k1[] = "k1";
  constexpr const ustore::byte_t v1[] = "v1";
  constexpr const ustore::byte_t k2[] = "k22";
  constexpr const ustore::byte_t v2[] = "v22";
  constexpr const ustore::byte_t k3[] = "k333";
  constexpr const ustore::byte_t v3[] = "v333";

  ustore::KVItem kv1{k1, v1, 2, 2};
  ustore::KVItem kv2{k2, v2, 3, 3};
  ustore::KVItem kv3{k3, v3, 4, 4};

  ustore::byte_t* seg_data1 = new ustore::byte_t[100];
  ustore::byte_t* seg_data2 = new ustore::byte_t[100];
  ustore::byte_t* seg_data3 = new ustore::byte_t[100];

  size_t kv1_num_bytes = ustore::MapNode::encode(seg_data1, kv1);
  size_t kv2_num_bytes = ustore::MapNode::encode(seg_data2, kv2);
  size_t kv3_num_bytes = ustore::MapNode::encode(seg_data3, kv3);

  size_t seg1_num_bytes = kv1_num_bytes;
  ustore::VarSegment seg1(seg_data1, seg1_num_bytes, {0});

  size_t seg2_num_bytes = kv2_num_bytes;
  ustore::VarSegment seg2(seg_data2, seg2_num_bytes, {0});

  size_t seg3_num_bytes = kv3_num_bytes;
  ustore::VarSegment seg3(seg_data3, seg3_num_bytes, {0});

  ustore::ChunkInfo chunk_info12 = ustore::MapChunker::Instance()
                                   ->make({&seg1, &seg2});
  ustore::ChunkInfo chunk_info3 = ustore::MapChunker::Instance()
                                   ->make({&seg3});

  ustore::ChunkInfo chunkinfo_meta = ustore::MetaChunker::Instance()
                                     ->make({chunk_info12.meta_seg.get(),
                                               chunk_info12.meta_seg.get()});

  ustore::ChunkStore* chunk_store = ustore::store::GetChunkStore();
  EXPECT_TRUE(chunk_store->Put(chunk_info12.chunk->hash(),
                               *(chunk_info12.chunk)));

  EXPECT_TRUE(chunk_store->Put(chunk_info3.chunk->hash(),
                               *(chunk_info3.chunk)));

  EXPECT_TRUE(chunk_store->Put(chunkinfo_meta.chunk->hash(),
                               *(chunkinfo_meta.chunk)));
  //////////////////////////////////////////////////////////////

  ustore::Hash root_hash = chunkinfo_meta.chunk->hash();

  ustore::ChunkLoader loader;

  // Find the smallest key
  bool found = true;
  constexpr ustore::byte_t k0[] = "k0";
  const ustore::OrderedKey key0(false, k0, 2);
  ustore::NodeCursor* cursor = ustore::NodeCursor::GetCursorByKey(
                                  root_hash, key0,
                                  &loader, &found);
  EXPECT_EQ(0, cursor->idx());
  EXPECT_FALSE(found);
  delete cursor;

  // Find the first exact key
  found = false;
  const ustore::OrderedKey key1(false, k1, 2);
  cursor = ustore::NodeCursor::GetCursorByKey(
                                  root_hash, key1,
                                  &loader, &found);
  EXPECT_EQ(0, cursor->idx());
  EXPECT_TRUE(found);
  delete cursor;

  // Find the last exact key in a chunk
  found = false;
  const ustore::OrderedKey key2(false, k2, 3);
  cursor = ustore::NodeCursor::GetCursorByKey(
                                  root_hash, key2,
                                  &loader, &found);
  EXPECT_EQ(1, cursor->idx());
  EXPECT_TRUE(found);
  delete cursor;

  // Find the non-exact key at start of chunk
  found = true;
  constexpr ustore::byte_t k23[] = "k23";
  const ustore::OrderedKey key23(false, k23, 3);
  cursor = ustore::NodeCursor::GetCursorByKey(
                                  root_hash, key23,
                                  &loader, &found);
  EXPECT_EQ(2, cursor->idx());
  EXPECT_FALSE(found);
  delete cursor;

  // Search until the last key
  found = true;
  constexpr ustore::byte_t k4[] = "k4";
  const ustore::OrderedKey key4(false, k4, 2);
  cursor = ustore::NodeCursor::GetCursorByKey(
                                  root_hash, key4,
                                  &loader, &found);
  EXPECT_EQ(2, cursor->idx());
  EXPECT_FALSE(found);
  delete cursor;
}
