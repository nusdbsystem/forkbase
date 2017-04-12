// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>

#include "chunk/chunk.h"
#include "node/cell_node.h"
#include "gtest/gtest.h"
#include "types/ustring.h"
#include "store/chunk_store.h"
#include "utils/debug.h"
#include "utils/logging.h"

const ustore::byte_t raw_data[] = "The quick brown fox jumps over the lazy dog";

TEST(UString, Load) {
  //////////////////////////////////////////////////////
  // Prepare the chunk to load
  const ustore::Chunk* chunk =
      ustore::StringNode::NewChunk(raw_data, sizeof(raw_data));

  ustore::ChunkStore* cs = ustore::store::GetChunkStore();
  // Put the chunk into storage
  cs->Put(chunk->hash(), *chunk);
  //////////////////////////////////////////////////////

  ustore::UString ustring = ustore::UString::Load(chunk->hash());
  ASSERT_EQ(sizeof(raw_data), ustring.len());

  ustore::byte_t* buffer = new ustore::byte_t[sizeof(raw_data)];
  ASSERT_EQ(sizeof(raw_data), ustring.data(buffer));

  std::string buf_str = ustore::byte2str(buffer, sizeof(raw_data));
  std::string expected_str = ustore::byte2str(raw_data, sizeof(raw_data));

  ASSERT_EQ(expected_str, buf_str);
}

TEST(UString, Create) {
  ustore::UString ustring = ustore::UString::Create(raw_data, sizeof(raw_data));

  ASSERT_EQ(sizeof(raw_data), ustring.len());

  ustore::byte_t* buffer = new ustore::byte_t[sizeof(raw_data)];
  ASSERT_EQ(sizeof(raw_data), ustring.data(buffer));

  std::string buf_str = ustore::byte2str(buffer, sizeof(raw_data));
  std::string expected_str = ustore::byte2str(raw_data, sizeof(raw_data));

  ASSERT_EQ(expected_str, buf_str);
}
