// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>

#include "gtest/gtest.h"

#include "node/list_node.h"

const ustore::Slice element1("aa", 2);
const ustore::Slice element2("bbb", 3);
const ustore::Slice element3("cccc", 4);

class ListNodeEnv : public ::testing::Test {
 protected:
  // Make a chunk for ListNode
  virtual void SetUp() {
    // Create a Segment with Element 1 and 2
    auto seg12 = ustore::ListNode::Encode({element1, element2});

    // Create a Segment with Entry 3
    auto seg3 = ustore::ListNode::Encode({element3});

    chunk_info_ = ustore::ListChunker::Instance()->Make({seg12.get(),
                                                         seg3.get()});
  }

  virtual void TearDown() { }

  ustore::ChunkInfo chunk_info_;
};

TEST_F(ListNodeEnv, Chunker) {
  ASSERT_EQ(1, chunk_info_.meta_seg->numEntries());
  const ustore::byte_t* me_data = chunk_info_.meta_seg->entry(0);
  ustore::MetaEntry me(me_data);

  EXPECT_EQ(chunk_info_.chunk->hash(), me.targetHash());
  EXPECT_EQ(chunk_info_.meta_seg->numBytes(), me.numBytes());
  EXPECT_EQ(3, me.numElements());
  EXPECT_EQ(1, me.numLeaves());
}

TEST_F(ListNodeEnv, ListNode) {
  chunk_info_.chunk.get()->hash();
  ustore::ListNode lnode(chunk_info_.chunk.get());
  EXPECT_EQ(3, lnode.numEntries());
  EXPECT_EQ(3, lnode.numElements());
  const ustore::Slice actual_element2 = ustore::ListNode::Decode(lnode.data(1));
  EXPECT_EQ(element2.len(), actual_element2.len());
  EXPECT_EQ(0,
            std::memcmp(element2.data(),
                        actual_element2.data(),
                        element2.len()));
}
