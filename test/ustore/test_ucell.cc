// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>

#include "chunk/chunk.h"
#include "store/chunk_store.h"
#include "node/cell_node.h"
#include "gtest/gtest.h"
#include "types/ucell.h"
#include "utils/logging.h"

TEST(UCell, Load) {
  ustore::Hash h1;
  h1.FromString("26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  ustore::UType type = ustore::kBlob;
  const ustore::Chunk* chunk = ustore::CellNode::NewChunk(type, h1);

  ustore::ChunkStore* cs = ustore::GetChunkStore();
  // Put the chunk into storage
  cs->Put(chunk->hash(), *chunk);

  const ustore::UCell* ucell = ustore::UCell::Load(chunk->hash());

  EXPECT_EQ(ucell->type(), type);
  EXPECT_EQ(ucell->merged(), false);
  EXPECT_EQ(ucell->dataHash(), h1);
  EXPECT_EQ(ucell->preUNodeHash(), ustore::Hash::NULL_HASH);
  EXPECT_TRUE(ucell->preUNodeHash(true).empty());
}

TEST(UCell, Create) {
  ustore::Hash h1, h2, h3;
  h1.FromString("26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  h2.FromString("36UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  h3.FromString("46UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  ustore::UType type = ustore::kBlob;

  const ustore::UCell* ucell = ustore::UCell::Create(type, h1,
                                                     h2, h3);

  EXPECT_EQ(ucell->type(), type);
  EXPECT_EQ(ucell->merged(), true);
  EXPECT_EQ(ucell->dataHash(), h1);
  EXPECT_EQ(ucell->preUNodeHash(), h2);
  EXPECT_EQ(ucell->preUNodeHash(true), h3);
}
