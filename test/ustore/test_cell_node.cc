// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>

#include "gtest/gtest.h"

#include "node/cell_node.h"

TEST(CellNode, NewCellNode) {
  auto h1 = ustore::Hash::FromBase32("26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  ustore::UType type = ustore::UType::kBlob;
  std::string cell_key("cell_key");
  ustore::Slice key(cell_key);
  ustore::Chunk chunk = ustore::CellNode::NewChunk(type, key, h1,
                                                   ustore::Hash::kNull);
  ustore::CellNode cnode(std::move(chunk));
  EXPECT_EQ(type, cnode.type());
  EXPECT_FALSE(cnode.merged());
  EXPECT_EQ(h1, cnode.dataHash());
  EXPECT_EQ(ustore::Hash::kNull, cnode.preHash());
  EXPECT_TRUE(cnode.preHash(true).empty());
  EXPECT_EQ(key.len(), cnode.keyLength());
  EXPECT_EQ(key, cnode.key());
}

TEST(CellNode, SinglePreHash) {
  auto h1 = ustore::Hash::FromBase32("26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  auto h2 = ustore::Hash::FromBase32("36UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  ustore::Hash h3;

  ustore::UType type = ustore::UType::kBlob;
  std::string cell_key("cell_key");
  ustore::Slice key(cell_key);
  ustore::Chunk chunk = ustore::CellNode::NewChunk(type, key, h1, h2, h3);
  ustore::CellNode cnode(std::move(chunk));

  EXPECT_EQ(type, cnode.type());
  EXPECT_FALSE(cnode.merged());
  EXPECT_EQ(h1, cnode.dataHash());
  EXPECT_EQ(h2, cnode.preHash());
  EXPECT_TRUE(cnode.preHash(true).empty());
  EXPECT_EQ(key.len(), cnode.keyLength());
  EXPECT_EQ(key, cnode.key());
}

TEST(CellNode, DoublePreHash) {
  auto h1 = ustore::Hash::FromBase32("26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  auto h2 = ustore::Hash::FromBase32("36UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  auto h3 = ustore::Hash::FromBase32("46UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");

  std::string cell_key("cell_key");
  ustore::Slice key(cell_key);
  ustore::UType type = ustore::UType::kBlob;
  ustore::Chunk chunk = ustore::CellNode::NewChunk(type, key, h1, h2, h3);
  ustore::CellNode cnode(std::move(chunk));

  EXPECT_EQ(type, cnode.type());
  EXPECT_TRUE(cnode.merged());
  EXPECT_EQ(h1, cnode.dataHash());
  EXPECT_EQ(h2, cnode.preHash());
  EXPECT_EQ(h3, cnode.preHash(true));
  EXPECT_EQ(key.len(), cnode.keyLength());
  EXPECT_EQ(key, cnode.key());
}
