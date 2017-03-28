// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>

#include "chunk/chunk.h"
#include "node/cell_node.h"
#include "gtest/gtest.h"

TEST(CellNode, NewCellNode) {
  ustore::Hash h1;
  h1.FromString("26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  ustore::UType type = ustore::UType::kBlob;
  const ustore::Chunk* chunk = ustore::CellNode::NewChunk(type, h1);

  ustore::CellNode cnode(chunk);
  EXPECT_EQ(cnode.type(), type);
  EXPECT_EQ(cnode.merged(), false);
  EXPECT_EQ(cnode.dataHash(), h1);
  EXPECT_EQ(cnode.preHash(), ustore::Hash::NULL_HASH);
  EXPECT_TRUE(cnode.preHash(true).empty());
}

TEST(CellNode, SinglePreHash) {
  ustore::Hash h1, h2, h3;
  h1.FromString("26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  h2.FromString("36UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");

  ustore::UType type = ustore::UType::kBlob;
  const ustore::Chunk* chunk = ustore::CellNode::NewChunk(type, h1, h2, h3);

  ustore::CellNode cnode(chunk);

  EXPECT_EQ(cnode.type(), type);
  EXPECT_EQ(cnode.merged(), false);
  EXPECT_EQ(cnode.dataHash(), h1);
  EXPECT_EQ(cnode.preHash(), h2);
  EXPECT_TRUE(cnode.preHash(true).empty());
}

TEST(CellNode, DoublePreHash) {
  ustore::Hash h1, h2, h3;
  h1.FromString("26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  h2.FromString("36UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  h3.FromString("46UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");

  ustore::UType type = ustore::UType::kBlob;
  const ustore::Chunk* chunk = ustore::CellNode::NewChunk(type, h1, h2, h3);

  ustore::CellNode cnode(chunk);

  EXPECT_EQ(cnode.type(), type);
  EXPECT_EQ(cnode.merged(), true);
  EXPECT_EQ(cnode.dataHash(), h1);
  EXPECT_EQ(cnode.preHash(), h2);
  EXPECT_EQ(cnode.preHash(true), h3);
}
