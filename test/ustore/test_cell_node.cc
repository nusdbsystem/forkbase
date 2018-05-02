// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>

#include "gtest/gtest.h"

#include "node/cell_node.h"

TEST(CellNode, NewCellNode) {
  auto data = ustore::Slice("26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");

  ustore::UType type = ustore::UType::kString;
  std::string cell_key("cell_key");
  std::string cell_ctx("cell_ctx");
  ustore::Slice key(cell_key);
  ustore::Slice ctx(cell_ctx);
  auto chunk = ustore::CellNode::NewChunk(type, key, data, ctx,
                                          ustore::Hash::kNull, ustore::Hash());
  ustore::CellNode cnode(std::move(chunk));

  EXPECT_EQ(type, cnode.type());
  EXPECT_EQ(size_t(1), cnode.numPreHash());
  EXPECT_EQ(data, ustore::Slice(cnode.data(), cnode.dataLength()));
  EXPECT_EQ(ustore::Hash::kNull, cnode.preHash(0));
  EXPECT_TRUE(cnode.preHash(1).empty());
  EXPECT_EQ(key.len(), cnode.keyLength());
  EXPECT_EQ(key, ustore::Slice(cnode.key(), cnode.keyLength()));
  EXPECT_EQ(ctx.len(), cnode.ctxLength());
  EXPECT_EQ(ctx, ustore::Slice(cnode.ctx(), cnode.ctxLength()));
}

TEST(CellNode, SinglePreHash) {
  auto data = ustore::Slice("26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  auto h2 = ustore::Hash::FromBase32("36UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  ustore::Hash h3;

  ustore::UType type = ustore::UType::kString;
  std::string cell_key("cell_key");
  std::string cell_ctx("cell_ctx");
  ustore::Slice key(cell_key);
  ustore::Slice ctx(cell_ctx);
  ustore::Chunk chunk = ustore::CellNode::NewChunk(type, key, data, ctx, h2,
                                                   h3);
  ustore::CellNode cnode(std::move(chunk));

  EXPECT_EQ(type, cnode.type());
  EXPECT_EQ(size_t(1), cnode.numPreHash());
  EXPECT_EQ(data, ustore::Slice(cnode.data(), cnode.dataLength()));
  EXPECT_EQ(h2, cnode.preHash(0));
  EXPECT_TRUE(cnode.preHash(1).empty());
  EXPECT_EQ(key.len(), cnode.keyLength());
  EXPECT_EQ(key, ustore::Slice(cnode.key(), cnode.keyLength()));
  EXPECT_EQ(ctx.len(), cnode.ctxLength());
  EXPECT_EQ(ctx, ustore::Slice(cnode.ctx(), cnode.ctxLength()));
}

TEST(CellNode, DoublePreHash) {
  auto data = ustore::Slice("26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  auto h2 = ustore::Hash::FromBase32("36UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  auto h3 = ustore::Hash::FromBase32("46UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");

  std::string cell_key("cell_key");
  std::string cell_ctx("cell_ctx");
  ustore::Slice key(cell_key);
  ustore::Slice ctx(cell_ctx);
  ustore::UType type = ustore::UType::kString;
  ustore::Chunk chunk = ustore::CellNode::NewChunk(type, key, data, ctx, h2,
                                                   h3);
  ustore::CellNode cnode(std::move(chunk));

  EXPECT_EQ(type, cnode.type());
  EXPECT_EQ(size_t(2), cnode.numPreHash());
  EXPECT_EQ(data, ustore::Slice(cnode.data(), cnode.dataLength()));
  EXPECT_EQ(h2, cnode.preHash(0));
  EXPECT_EQ(h3, cnode.preHash(1));
  EXPECT_EQ(key.len(), cnode.keyLength());
  EXPECT_EQ(key, ustore::Slice(cnode.key(), cnode.keyLength()));
  EXPECT_EQ(ctx.len(), cnode.ctxLength());
  EXPECT_EQ(ctx, ustore::Slice(cnode.ctx(), cnode.ctxLength()));
}
