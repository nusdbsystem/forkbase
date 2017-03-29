// Copyright (c) 2017 The Ustore Authors.
#include <cstring>
#include <string>
#include <sstream>
#include "gtest/gtest.h"
#include "spec/value.h"

const char raw_str[] = "The quick brown fox jumps over the lazy dog";
const ustore::byte_t raw_blob[] = "This is a blob";

TEST(Value, FromString) {
  ustore::Slice slice(raw_str);
  ustore::Value value(slice);
  EXPECT_EQ(ustore::UType::kString, value.type());
  EXPECT_EQ(slice.len(), value.slice().len());
  EXPECT_EQ(slice, value.slice());
}

TEST(Value, FromBlob) {
  ustore::Blob blob(raw_blob, sizeof(raw_blob));
  ustore::Value value(blob);
  EXPECT_EQ(ustore::UType::kBlob, value.type());
  EXPECT_EQ(blob.size(), value.blob().size());
  EXPECT_EQ(0, std::memcmp(blob.data(), value.blob().data(), blob.size()));
}

TEST(Value, FromValue) {
  ustore::Slice slice(raw_str);
  ustore::Value value(slice);
  ustore::Value value2(value);
  EXPECT_EQ(ustore::UType::kString, value2.type());
  EXPECT_EQ(slice.len(), value2.slice().len());
  EXPECT_EQ(slice, value2.slice());
  ustore::Value value3 = value;
  EXPECT_EQ(ustore::UType::kString, value3.type());
  EXPECT_EQ(slice.len(), value3.slice().len());
  EXPECT_EQ(slice, value3.slice());
}
