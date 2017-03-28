// Copyright (c) 2017 The Ustore Authors.
#include <algorithm>
#include <cstring>
#include <string>
#include "gtest/gtest.h"

#include "node/chunk_loader.h"
#include "node/cursor.h"
#include "utils/singleton.h"

// NOTE: Haven't test GetCursorByKey
TEST(NodeCursor, SingleNode) {
  // Construct a tree with only a root blob node
  const ustore::byte_t ra[] = "abc";

  size_t ra_num_bytes = sizeof(ra) - 1;  // excluding trailing \0
  ustore::Chunk ca(ustore::ChunkType::kBlob, ra_num_bytes);
  std::copy(ra, ra + ra_num_bytes, ca.m_data());

  ustore::ChunkStore* chunk_store = ustore::GetChunkStore();
  // Write the constructed chunk to storage
  EXPECT_TRUE(chunk_store->Put(ca.hash(), ca));

  ustore::ChunkLoader loader(chunk_store);
  ustore::NodeCursor* cr =
          ustore::NodeCursor::GetCursorByIndex(ca.hash(), 1, &loader);

  ASSERT_EQ(*(cr->current()), 'b');
  EXPECT_EQ(cr->numCurrentBytes(), 1);

  EXPECT_FALSE(cr->Retreat(false));  // point the 0th element

  EXPECT_TRUE(cr->Retreat(false));  // point to seq head
  EXPECT_TRUE(cr->Retreat(false));  // still point to seq head
  EXPECT_TRUE(cr->Retreat(true));  // still point to seq head

  EXPECT_EQ(cr->current(), nullptr);
  EXPECT_EQ(cr->numCurrentBytes(), 0);
  EXPECT_TRUE(cr->isBegin());

  EXPECT_FALSE(cr->Advance(false));  // point the 0th element
  ASSERT_EQ(*(cr->current()), 'a');

  EXPECT_FALSE(cr->Advance(false));  // point to 1st element
  ASSERT_EQ(*(cr->current()), 'b');
  EXPECT_FALSE(cr->Advance(false));  // point to 2nd element
  ASSERT_EQ(*(cr->current()), 'c');

  EXPECT_TRUE(cr->Advance(false));  // point to seq end
  EXPECT_TRUE(cr->Advance(false));  // point to seq end

  EXPECT_EQ(cr->current(), nullptr);
  EXPECT_TRUE(cr->isEnd());
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
  const ustore::byte_t* me_a = ustore::MetaEntry::Encode(ra_num_bytes,
                                                         ra_num_bytes,
                                                         ca.hash(), ka,
                                                         &mta_numBytes);


  const ustore::byte_t rb[] = "cde";
  size_t rb_num_bytes = sizeof(rb) - 1;  // excluding trailing \0
  ustore::Chunk cb(ustore::ChunkType::kBlob, rb_num_bytes);
  std::copy(rb, rb + rb_num_bytes, cb.m_data());
  ustore::OrderedKey kb(2);

  size_t mtb_numBytes = 0;
  const ustore::byte_t* me_b = ustore::MetaEntry::Encode(rb_num_bytes,
                                                         rb_num_bytes,
                                                         cb.hash(), kb,
                                                         &mtb_numBytes);

  // Create MetaNode Chunk
  ustore::ChunkInfo cm_info = ustore::MetaNode::MakeChunk({me_a, me_b},
                                                     {mta_numBytes,
                                                     mta_numBytes});

  // delete the useless MetaEntry bytes
  delete[] cm_info.second.first;

  const ustore::Chunk* cm = cm_info.first;

  ustore::ChunkStore* chunk_store = ustore::GetChunkStore();

  // Write the constructed chunk to storage
  EXPECT_TRUE(chunk_store->Put(ca.hash(), ca));
  EXPECT_TRUE(chunk_store->Put(cb.hash(), cb));
  EXPECT_TRUE(chunk_store->Put(cm->hash(), *cm));

///////////////////////////////////////////////////////////////
  ustore::ChunkLoader loader(chunk_store);

  ustore::NodeCursor* leaf_cursor =
          ustore::NodeCursor::GetCursorByIndex(cm->hash(), 1, &loader);

  ASSERT_NE(leaf_cursor, nullptr);
  EXPECT_EQ(leaf_cursor->numCurrentBytes(), 1);
  EXPECT_EQ(*leaf_cursor->current(), 'b');

  ustore::NodeCursor* cr_copy = new ustore::NodeCursor(*leaf_cursor);

  //// Test Advancing
  // Advance cursor to reach leaf node end
  EXPECT_TRUE(leaf_cursor->Advance(false));
  // Advance from leaf node end
  EXPECT_TRUE(leaf_cursor->Advance(false));

  // Advance to the next chunk
  EXPECT_FALSE(leaf_cursor->Advance(true));
  EXPECT_EQ(*leaf_cursor->current(), 'c');

  // leaf cursor retreats to seq start
  EXPECT_TRUE(leaf_cursor->Retreat(false));
  EXPECT_FALSE(leaf_cursor->Retreat(true));
  EXPECT_EQ(*leaf_cursor->current(), 'b');


  // Cross the boundary to reach the second blob node
  EXPECT_FALSE(cr_copy->Advance(true));

  // the original cursor does not change
  EXPECT_EQ(*leaf_cursor->current(), 'b');
  EXPECT_EQ(*cr_copy->current(), 'c');
  EXPECT_FALSE(cr_copy->Advance(true));
  EXPECT_FALSE(cr_copy->Advance(true));

  // cursor points to Node end after advancing
  EXPECT_TRUE(cr_copy->Advance(true));
  EXPECT_EQ(cr_copy->current(), nullptr);
  EXPECT_EQ(cr_copy->numCurrentBytes(), 0);
  // cursor points to Node end before advancing
  EXPECT_TRUE(cr_copy->Advance(true));

  //// Test Retreating
  ustore::NodeCursor* cur2 =
          ustore::NodeCursor::GetCursorByIndex(cm->hash(), 3, &loader);
  EXPECT_EQ(*cur2->current(), 'd');
  // // points to the frist element after the following retreat
  EXPECT_FALSE(cur2->Retreat(false));
  // // Point to seq start after the following retreat.
  EXPECT_TRUE(cur2->Retreat(false));
  // // Still Point to seq start after the following retreat.
  EXPECT_TRUE(cur2->Retreat(false));

  // // // Cross the boundary and point to the first node
  EXPECT_FALSE(cur2->Retreat(true));
  EXPECT_EQ(*cur2->current(), 'b');

  EXPECT_FALSE(cur2->Retreat(true));
  EXPECT_EQ(*cur2->current(), 'a');

  // // Point to Seq Start
  EXPECT_TRUE(cur2->Retreat(true));
  EXPECT_EQ(cur2->current(), nullptr);

  // // last element
  ustore::NodeCursor* cur3 =
          ustore::NodeCursor::GetCursorByIndex(cm->hash(), 4, &loader);
  EXPECT_EQ(*cur3->current(), 'e');


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
