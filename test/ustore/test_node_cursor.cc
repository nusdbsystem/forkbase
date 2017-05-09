// Copyright (c) 2017 The Ustore Authors.
#include <algorithm>
#include <cstring>
#include <string>

#include "gtest/gtest.h"

#include "node/cursor.h"
#include "node/blob_node.h"
#include "node/map_node.h"
#include "node/node_builder.h"

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
  delete cr;


  // Check for multi-step advancing
  ustore::NodeCursor* multi_cr =
      ustore::NodeCursor::GetCursorByIndex(ca.hash(), 0, &loader);

  EXPECT_EQ(2, multi_cr->AdvanceSteps(2));
  ASSERT_EQ('c', *(multi_cr->current()));

  // Advance to seq end
  EXPECT_EQ(1, multi_cr->AdvanceSteps(2));
  ASSERT_TRUE(multi_cr->isEnd());
  delete multi_cr;

  // Check Advance from Seq Start
  ustore::NodeCursor* multi_cr1 =
      ustore::NodeCursor::GetCursorByIndex(ca.hash(), 0, &loader);

  ASSERT_FALSE(multi_cr1->Retreat(false));
  ASSERT_TRUE(multi_cr1->isBegin());

  // Advance to seq end
  EXPECT_EQ(4, multi_cr1->AdvanceSteps(5));
  ASSERT_TRUE(multi_cr1->isEnd());

  delete multi_cr1;

  // Check for multi-step retreating
  ustore::NodeCursor* multi_cr2 =
      ustore::NodeCursor::GetCursorByIndex(ca.hash(), 2, &loader);

  EXPECT_EQ(2, multi_cr2->RetreatSteps(2));
  ASSERT_EQ('a', *(multi_cr2->current()));

  // Retreat to seq end
  EXPECT_EQ(1, multi_cr2->RetreatSteps(2));
  ASSERT_TRUE(multi_cr2->isBegin());
  delete multi_cr2;

  // Check Advance from Seq End
  ustore::NodeCursor* multi_cr3 =
      ustore::NodeCursor::GetCursorByIndex(ca.hash(), 2, &loader);

  ASSERT_FALSE(multi_cr3->Advance(false));
  ASSERT_TRUE(multi_cr3->isEnd());

  // Advance to seq end
  EXPECT_EQ(4, multi_cr3->RetreatSteps(5));
  ASSERT_TRUE(multi_cr3->isBegin());

  delete multi_cr3;
}

TEST(NodeCursor, Tree) {
  // Construct a tree with two blob nodes as leaves.
  //   first blob node contains "ab"
  //   second blob node contains "cde"
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
      ustore::MetaChunker::Instance()->Make({&seg_a, &seg_b});
  // delete the useless MetaEntry bytes

  ustore::Chunk cm(std::move(cm_info.chunk));

  ustore::ChunkStore* chunk_store = ustore::store::GetChunkStore();

  // Write the constructed chunk to storage
  EXPECT_TRUE(chunk_store->Put(ca.hash(), ca));
  EXPECT_TRUE(chunk_store->Put(cb.hash(), cb));
  EXPECT_TRUE(chunk_store->Put(cm.hash(), cm));

  ///////////////////////////////////////////////////////////////
  ustore::ChunkLoader loader;

  ustore::NodeCursor* leaf_cursor =
      ustore::NodeCursor::GetCursorByIndex(cm.hash(), 1, &loader);

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
      ustore::NodeCursor::GetCursorByIndex(cm.hash(), 3, &loader);
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
      ustore::NodeCursor::GetCursorByIndex(cm.hash(), 4, &loader);
  EXPECT_EQ('e', *cur3->current());

  // test the case when index exceeds num elements
  ustore::NodeCursor* cur4 =
      ustore::NodeCursor::GetCursorByIndex(cm.hash(), 5, &loader);
  EXPECT_TRUE(cur4->isEnd());

  ustore::NodeCursor* cur5 =
      ustore::NodeCursor::GetCursorByIndex(cm.hash(), 6, &loader);
  EXPECT_TRUE(cur5 == nullptr);

  delete leaf_cursor;
  delete cr_copy;
  delete cur2;
  delete cur3;
  delete cur4;
  delete cur5;
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

  size_t kv1_num_bytes = ustore::MapNode::Encode(seg_data12, kv1);
  size_t kv2_num_bytes = ustore::MapNode::Encode(seg_data12
                                                 + kv1_num_bytes, kv2);
  size_t kv3_num_bytes = ustore::MapNode::Encode(seg_data3, kv3);

  size_t seg12_num_bytes = kv1_num_bytes + kv2_num_bytes;
  ustore::VarSegment seg12(seg_data12, seg12_num_bytes, {0, kv1_num_bytes});

  size_t seg3_num_bytes = kv3_num_bytes;
  ustore::VarSegment seg3(seg_data3, seg3_num_bytes, {0});

  std::vector<const ustore::Segment*> segs {&seg12, &seg3};
  ustore::ChunkInfo chunk_info = ustore::MapChunker::Instance()->Make(segs);

  ustore::Chunk chunk(std::move(chunk_info.chunk));
  ustore::ChunkStore* chunk_store = ustore::store::GetChunkStore();
  ustore::ChunkLoader loader;
  EXPECT_TRUE(chunk_store->Put(chunk.hash(), chunk));

  // Find the smallest key
  bool found = true;
  constexpr ustore::byte_t k0[] = "k0";
  const ustore::OrderedKey key0(false, k0, 2);
  ustore::NodeCursor* cursor = ustore::NodeCursor::GetCursorByKey(
                                  chunk.hash(), key0, &loader);
  EXPECT_EQ(0, cursor->idx());
  delete cursor;

  // Find the exact key
  found = false;
  const ustore::OrderedKey key1(false, k1, 2);
  cursor = ustore::NodeCursor::GetCursorByKey(chunk.hash(), key1, &loader);
  EXPECT_EQ(0, cursor->idx());
  delete cursor;

  // Find the non-exact key
  found = true;
  constexpr ustore::byte_t k12[] = "k12";
  const ustore::OrderedKey key12(false, k12, 3);
  cursor = ustore::NodeCursor::GetCursorByKey(chunk.hash(), key12, &loader);
  EXPECT_EQ(1, cursor->idx());
  delete cursor;

  // Find the non-exact key
  found = true;
  constexpr ustore::byte_t k4[] = "k4";
  const ustore::OrderedKey key4(false, k4, 2);
  cursor = ustore::NodeCursor::GetCursorByKey(chunk.hash(), key4, &loader);
  EXPECT_EQ(3, cursor->idx());
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

  size_t kv1_num_bytes = ustore::MapNode::Encode(seg_data1, kv1);
  size_t kv2_num_bytes = ustore::MapNode::Encode(seg_data2, kv2);
  size_t kv3_num_bytes = ustore::MapNode::Encode(seg_data3, kv3);

  size_t seg1_num_bytes = kv1_num_bytes;
  ustore::VarSegment seg1(seg_data1, seg1_num_bytes, {0});

  size_t seg2_num_bytes = kv2_num_bytes;
  ustore::VarSegment seg2(seg_data2, seg2_num_bytes, {0});

  size_t seg3_num_bytes = kv3_num_bytes;
  ustore::VarSegment seg3(seg_data3, seg3_num_bytes, {0});

  ustore::ChunkInfo chunk_info12 = ustore::MapChunker::Instance()
                                   ->Make({&seg1, &seg2});
  ustore::ChunkInfo chunk_info3 = ustore::MapChunker::Instance()
                                   ->Make({&seg3});

  ustore::ChunkInfo chunkinfo_meta = ustore::MetaChunker::Instance()
                                     ->Make({chunk_info12.meta_seg.get(),
                                               chunk_info12.meta_seg.get()});

  ustore::ChunkStore* chunk_store = ustore::store::GetChunkStore();
  EXPECT_TRUE(chunk_store->Put(chunk_info12.chunk.hash(),
                               chunk_info12.chunk));

  EXPECT_TRUE(chunk_store->Put(chunk_info3.chunk.hash(),
                               chunk_info3.chunk));

  EXPECT_TRUE(chunk_store->Put(chunkinfo_meta.chunk.hash(),
                               chunkinfo_meta.chunk));
  //////////////////////////////////////////////////////////////

  ustore::Hash root_hash = chunkinfo_meta.chunk.hash();

  ustore::ChunkLoader loader;

  // Find the smallest key
  constexpr ustore::byte_t k0[] = "k0";
  const ustore::OrderedKey key0(false, k0, 2);
  ustore::NodeCursor* cursor = ustore::NodeCursor::GetCursorByKey(
                                  root_hash, key0, &loader);
  EXPECT_EQ(0, cursor->idx());
  delete cursor;

  // Find the first exact key
  const ustore::OrderedKey key1(false, k1, 2);
  cursor = ustore::NodeCursor::GetCursorByKey(root_hash, key1, &loader);
  EXPECT_EQ(0, cursor->idx());
  delete cursor;

  // Find the last exact key in a chunk
  const ustore::OrderedKey key2(false, k2, 3);
  cursor = ustore::NodeCursor::GetCursorByKey(root_hash, key2, &loader);
  EXPECT_EQ(1, cursor->idx());
  delete cursor;

  // Find the non-exact key at start of chunk
  constexpr ustore::byte_t k23[] = "k23";
  const ustore::OrderedKey key23(false, k23, 3);
  cursor = ustore::NodeCursor::GetCursorByKey(root_hash, key23, &loader);
  EXPECT_EQ(2, cursor->idx());
  delete cursor;

  // Search until the last key
  constexpr ustore::byte_t k4[] = "k4";
  const ustore::OrderedKey key4(false, k4, 2);
  cursor = ustore::NodeCursor::GetCursorByKey(root_hash, key4, &loader);
  EXPECT_EQ(2, cursor->idx());
  delete cursor;
}

TEST(NodeCursor, MultiStep) {
  const ustore::byte_t raw_data[] = {
        "SCENE I. Rome. A street.  Enter FLAVIUS, MARULLUS, and certain "
        "Commoners FLAVIUS Hence! home, you idle creatures get you home: Is "
        "this "
        "a holiday? what! know you not, Being mechanical, you ought not walk "
        "Upon a labouring day without the sign Of your profession? Speak, what "
        "trade art thou?  First Commoner Why, sir, a carpenter.  MARULLUS "
        "Where "
        "is thy leather apron and thy rule?  What dost thou with thy best "
        "apparel on?  You, sir, what trade are you?  Second Commoner Truly, "
        "sir, "
        "in respect of a fine workman, I am but, as you would say, a cobbler.  "
        "MARULLUS But what trade art thou? answer me directly.  Second "
        "Commoner "
        "I am, indeed, sir, a surgeon to old shoes; when they are in great "
        "danger, I recover them. As proper men as ever trod upon neat's "
        "leather "
        "have gone upon my handiwork.  FLAVIUS But wherefore art not in thy "
        "shop "
        "today?  Why dost thou lead these men about the streets?  Second "
        "Commoner Truly, sir, to wear out their shoes, to get myself into more "
        "work. But, indeed, sir, we make holiday, to see Caesar and to rejoice "
        "in his triumph.  MARULLUS Wherefore rejoice? What conquest brings he "
        "home?  What tributaries follow him to Rome, To grace in captive bonds "
        "his chariot-wheels?  You blocks, you stones, you worse than senseless "
        "things!  O you hard hearts, you cruel men of Rome, Knew you not "
        "Pompey? "
        "Many a time and oft Have you climb'd up to walls and battlements, To "
        "towers and windows, yea, to chimney-tops, Your infants in your arms, "
        "Caesar's trophies. I'll about, And drive away the vulgar from the "
        "streets: So do you too, where you perceive them thick.  These growing "
        "feathers pluck'd from Caesar's wing Will make him fly an ordinary "
        "pitch, Who else would soar above the view of men And keep us all in "
        "servile fearfulness. Exeunt"};

  size_t num_bytes = sizeof(raw_data) - 1;
  ustore::byte_t* content = new ustore::byte_t[num_bytes];
  std::memcpy(content, raw_data, num_bytes);

  const ustore::Chunker* chunker = ustore::BlobChunker::Instance();
  ustore::NodeBuilder builder(chunker, true);

  ustore::FixedSegment seg(content, num_bytes, 1);

  builder.SpliceElements(0, &seg);
  const ustore::Hash root = builder.Commit();

  /* Setup a normal Prolley tree as followed:
                      2                       (# of Entry in MetaChunk)
  |---------------------------------------|
    2                   8                     (# of Entry in MetaChunk)
  |--- |  |-------------------------------|
  67  38  193  183  512  320  53  55  74  256 (# of Byte in BlobChunk)
  */

  ustore::ChunkLoader loader;


/////////////////////////////////////////
// Test on Advancing
  ustore::NodeCursor* cr1 =
      ustore::NodeCursor::GetCursorByIndex(root, 0, &loader);

  // Advance to the third leaf chunk first element
  ASSERT_EQ(67 + 38, cr1->AdvanceSteps(67 + 38));
  ASSERT_EQ(0, std::memcmp(raw_data + 67 + 38, cr1->current(), 193));
  delete cr1;

  ustore::NodeCursor* cr2 =
      ustore::NodeCursor::GetCursorByIndex(root, 0, &loader);

  // Advance to the third leaf chunk last element
  size_t step = 67 + 38 + 192;
  ASSERT_EQ(step, cr2->AdvanceSteps(step));
  ASSERT_EQ(raw_data[step], *cr2->current());
  delete cr2;

  // Advance 1 step across chunk boundary
  ustore::NodeCursor* cr3 =
      ustore::NodeCursor::GetCursorByIndex(root, 67 + 37, &loader);

  // Advance to the third leaf chunk first element
  ASSERT_EQ(1, cr3->AdvanceSteps(1));
  ASSERT_EQ(0, std::memcmp(raw_data + 67 + 38, cr3->current(), 193));
  delete cr3;

  // Advance from first to the last element
  ustore::NodeCursor* cr4 =
      ustore::NodeCursor::GetCursorByIndex(root, 0, &loader);

  ASSERT_EQ(num_bytes - 1, cr4->AdvanceSteps(num_bytes - 1));
  ASSERT_EQ(raw_data[num_bytes - 1], *cr4->current());
  delete cr4;

  // Advance from first to the seq end with overflow
  ustore::NodeCursor* cr5 =
      ustore::NodeCursor::GetCursorByIndex(root, 0, &loader);

  ASSERT_EQ(num_bytes, cr5->AdvanceSteps(num_bytes + 5));
  ASSERT_TRUE(cr5->isEnd());
  delete cr5;

  // Advance from seq head to end
  ustore::NodeCursor* cr6 =
      ustore::NodeCursor::GetCursorByIndex(root, 0, &loader);
  ASSERT_FALSE(cr6->Retreat(false));

  ASSERT_EQ(num_bytes + 1, cr6->AdvanceSteps(num_bytes + 5));
  ASSERT_TRUE(cr6->isEnd());
  delete cr6;

///////////////////////////////////////
// Check for Retreating
  // Place cursor at first element of third leaf chunk
  ustore::NodeCursor* cr7 =
      ustore::NodeCursor::GetCursorByIndex(root, 67 + 38, &loader);

  // Retreat 1 step to last element of the second chunk
  ASSERT_EQ(1, cr7->RetreatSteps(1));
  ASSERT_EQ(raw_data[67 + 37], *cr7->current());
  delete cr7;

  // Place cursor at first element of third leaf chunk
  ustore::NodeCursor* cr8 =
      ustore::NodeCursor::GetCursorByIndex(root, 67 + 38, &loader);

  // Retreat 38 step to first element of the second chunk
  ASSERT_EQ(38, cr8->RetreatSteps(38));
  ASSERT_EQ(0, std::memcmp(raw_data + 67, cr8->current(), 38));
  delete cr8;

  // Place cursor at seq end
  ustore::NodeCursor* cr9 =
      ustore::NodeCursor::GetCursorByIndex(root, num_bytes, &loader);
  ASSERT_TRUE(cr9->isEnd());

  // Retreat to first element of the first chunk
  ASSERT_EQ(num_bytes, cr9->RetreatSteps(num_bytes));
  ASSERT_EQ(0, std::memcmp(raw_data, cr9->current(), 67));
  delete cr9;

  // Place cursor at seq end
  ustore::NodeCursor* cr10 =
      ustore::NodeCursor::GetCursorByIndex(root, num_bytes, &loader);
  ASSERT_TRUE(cr10->isEnd());

  // Retreat to head of the first chunk
  ASSERT_EQ(num_bytes + 1, cr10->RetreatSteps(num_bytes + 5));
  ASSERT_TRUE(cr10->isBegin());
  delete cr10;
}
