// Copyright (c) 2017 The Ustore Authors.
#include <algorithm>
#include <cstring>
#include <string>
#include "gtest/gtest.h"

#include "chunk/chunker.h"
#include "node/cursor.h"
#include "store/chunk_loader.h"
#include "utils/singleton.h"

// NOTE: Haven't test GetCursorByKey
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
      ustore::Singleton<ustore::MetaChunker>::Instance()->make(
          {&seg_a, &seg_b});
  // delete the useless MetaEntry bytes
  delete cm_info.meta_seg;

  const ustore::Chunk* cm = cm_info.chunk;

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
}
