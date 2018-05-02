// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>

#include "gtest/gtest.h"

#include "types/ucell.h"
#include "store/chunk_store.h"
// #include "utils/logging.h"

TEST(UCell, Load) {
  auto h1 = ustore::Hash::FromBase32("26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  auto h1_slice = ustore::Slice(h1.value(), ustore::Hash::kByteLength);
  std::string cell_key("cell_key");
  std::string cell_ctx("cell_ctx");
  ustore::Slice key(cell_key);
  ustore::Slice ctx(cell_ctx);
  ustore::UType type = ustore::UType::kBlob;

  auto chunk = ustore::CellNode::NewChunk(type, key, h1_slice, ctx,
      ustore::Hash::kNull, ustore::Hash());

  ustore::ChunkStore* cs = ustore::store::GetChunkStore();
  // Put the chunk into storage
  cs->Put(chunk.hash(), chunk);

  ustore::UCell ucell = ustore::UCell::Load(chunk.hash());

  EXPECT_EQ(type, ucell.type());
  EXPECT_FALSE(ucell.merged());
  EXPECT_EQ(h1, ucell.dataHash());
  EXPECT_EQ(ustore::Hash::kNull, ucell.preHash());
  EXPECT_EQ(key, ucell.key());
  EXPECT_EQ(ctx, ucell.context());

  EXPECT_TRUE(ucell.preHash(true).empty());
}

TEST(UCell, Create) {
  auto h1 = ustore::Hash::FromBase32("26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  auto h2 = ustore::Hash::FromBase32("36UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  auto h3 = ustore::Hash::FromBase32("46UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  ustore::UType type = ustore::UType::kBlob;
  std::string cell_key("cell_key");
  std::string cell_ctx("cell_ctx");
  ustore::Slice key(cell_key);
  ustore::Slice ctx(cell_ctx);

  ustore::UCell ucell = ustore::UCell::Create(type, key, h1, ctx, h2, h3);

  EXPECT_EQ(type, ucell.type());
  EXPECT_TRUE(ucell.merged());
  EXPECT_EQ(h1, ucell.dataHash());
  EXPECT_EQ(h2, ucell.preHash());
  EXPECT_EQ(h3, ucell.preHash(true));
  EXPECT_EQ(key, ucell.key());
  EXPECT_EQ(ctx, ucell.context());
}

TEST(UCell, LoadFromByteArray) {
  auto h1 = ustore::Hash::FromBase32("26UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  auto h2 = ustore::Hash::FromBase32("36UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  auto h3 = ustore::Hash::FromBase32("46UPXMYH26AJI2OKTK6LACBOJ6GVMUPE");
  ustore::UType type = ustore::UType::kBlob;
  std::string cell_key("cell_key");
  std::string cell_ctx("cell_ctx");
  ustore::Slice key(cell_key);
  ustore::Slice ctx(cell_ctx);

  ustore::UCell ucell = ustore::UCell::Create(type, key, h1, ctx, h2, h3);
  const auto& chunk = ucell.chunk();
  std::unique_ptr<ustore::byte_t[]> buf(new ustore::byte_t[chunk.numBytes()]);
  std::memcpy(buf.get(), chunk.head(), chunk.numBytes());
  EXPECT_EQ(0, std::memcmp(buf.get(), chunk.head(), chunk.numBytes()));
  ucell = ustore::UCell(ustore::Chunk(std::move(buf)));
  EXPECT_EQ(type, ucell.type());
  EXPECT_TRUE(ucell.merged());
  EXPECT_EQ(h1, ucell.dataHash());
  EXPECT_EQ(h2, ucell.preHash());
  EXPECT_EQ(h3, ucell.preHash(true));
  EXPECT_EQ(key, ucell.key());
  EXPECT_EQ(ctx, ucell.context());
}
