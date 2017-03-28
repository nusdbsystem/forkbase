// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>

#include "chunk/chunk.h"
#include "node/string_node.h"
#include "gtest/gtest.h"
#include "utils/debug.h"

TEST(StringNode, Basic) {
  const ustore::byte_t raw_data[] =
      "The quick brown fox jumps over the lazy dog";

  const ustore::Chunk* chunk =
      ustore::StringNode::NewChunk(raw_data, sizeof(raw_data));

  ustore::StringNode snode(chunk);

  EXPECT_EQ(sizeof(raw_data), snode.len());
  ustore::byte_t* buffer = new ustore::byte_t[sizeof(raw_data)];
  ASSERT_EQ(sizeof(raw_data), snode.Copy(buffer));

  std::string buf_str = ustore::byte2str(buffer, sizeof(raw_data));
  std::string chunk_str = ustore::byte2str(
      chunk->data() + sizeof(uint32_t), chunk->capacity() - sizeof(uint32_t));

  ASSERT_EQ(chunk_str, buf_str);
}
