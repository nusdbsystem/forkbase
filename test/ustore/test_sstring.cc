// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>

#include "gtest/gtest.h"

#include "types/server/sstring.h"

#include "utils/debug.h"
// #include "utils/logging.h"

const ustore::byte_t raw_data[] = "The quick brown fox jumps over the lazy dog";

TEST(SString, Load) {
  //////////////////////////////////////////////////////
  // Prepare the chunk to load
  ustore::Chunk chunk =
    ustore::StringNode::NewChunk(raw_data, sizeof(raw_data));

  ustore::ChunkStore* cs = ustore::store::GetChunkStore();
  // Put the chunk into storage
  cs->Put(chunk.hash(), chunk);
  //////////////////////////////////////////////////////

  ustore::SString sstring(chunk.hash());
  ASSERT_EQ(sizeof(raw_data), sstring.len());

  ustore::byte_t* buffer = new ustore::byte_t[sizeof(raw_data)];
  ASSERT_EQ(sizeof(raw_data), sstring.data(buffer));

  std::string buf_str = ustore::byte2str(buffer, sizeof(raw_data));
  std::string expected_str = ustore::byte2str(raw_data, sizeof(raw_data));

  ASSERT_EQ(expected_str, buf_str);
  delete[] buffer;

  // Test for move ctor
  ustore::SString sstring1(std::move(sstring));
  ASSERT_EQ(sizeof(raw_data), sstring1.len());

  buffer = new ustore::byte_t[sizeof(raw_data)];
  ASSERT_EQ(sizeof(raw_data), sstring1.data(buffer));

  buf_str = ustore::byte2str(buffer, sizeof(raw_data));
  expected_str = ustore::byte2str(raw_data, sizeof(raw_data));

  ASSERT_EQ(expected_str, buf_str);
  delete[] buffer;

  // Test for move assignment
  ustore::SString sstring2;
  sstring2 = std::move(sstring1);
  ASSERT_EQ(sizeof(raw_data), sstring2.len());

  buffer = new ustore::byte_t[sizeof(raw_data)];
  ASSERT_EQ(sizeof(raw_data), sstring2.data(buffer));

  buf_str = ustore::byte2str(buffer, sizeof(raw_data));
  expected_str = ustore::byte2str(raw_data, sizeof(raw_data));

  ASSERT_EQ(expected_str, buf_str);
  delete[] buffer;
}

TEST(SString, Create) {
  ustore::Slice slice(raw_data, sizeof(raw_data));

  ustore::SString sstring(slice);

  ASSERT_EQ(sizeof(raw_data), sstring.len());

  ustore::byte_t* buffer = new ustore::byte_t[sizeof(raw_data)];
  ASSERT_EQ(sizeof(raw_data), sstring.data(buffer));

  std::string buf_str = ustore::byte2str(buffer, sizeof(raw_data));
  std::string expected_str = ustore::byte2str(raw_data, sizeof(raw_data));

  ASSERT_EQ(expected_str, buf_str);
}

TEST(SString, Empty) {
  ustore::Slice empty;
  ustore::SString sstring(empty);

  ASSERT_EQ(0, sstring.len());
  ASSERT_EQ(nullptr, sstring.data());
}
