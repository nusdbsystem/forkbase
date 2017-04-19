// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>

#include "chunk/chunk.h"
#include "gtest/gtest.h"
#include "node/cell_node.h"
#include "store/chunk_store.h"
#include "types/ucell.h"
#include "utils/logging.h"

TEST(UCell, Load) {
  ustore::Hash h1;
  h1.FromBase32("26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  ustore::UType type = ustore::UType::kBlob;
  const ustore::Chunk* chunk =
      ustore::CellNode::NewChunk(type, h1, ustore::Hash::kNull);

  ustore::ChunkStore* cs = ustore::store::GetChunkStore();
  // Put the chunk into storage
  cs->Put(chunk->hash(), *chunk);

  ustore::UCell ucell = ustore::UCell::Load(chunk->hash());

  EXPECT_EQ(type, ucell.type());
  EXPECT_FALSE(ucell.merged());
  EXPECT_EQ(h1, ucell.dataHash());
  EXPECT_EQ(ustore::Hash::kNull, ucell.preUCellHash());

  EXPECT_TRUE(ucell.preUCellHash(true).empty());
}

TEST(UCell, Create) {
  ustore::Hash h1, h2, h3;
  h1.FromBase32("26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  h2.FromBase32("36UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  h3.FromBase32("46UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  ustore::UType type = ustore::UType::kBlob;

  ustore::UCell ucell = ustore::UCell::Create(type, h1, h2, h3);

  EXPECT_EQ(type, ucell.type());
  EXPECT_TRUE(ucell.merged());
  EXPECT_EQ(h1, ucell.dataHash());
  EXPECT_EQ(h2, ucell.preUCellHash());
  EXPECT_EQ(h3, ucell.preUCellHash(true));
}
