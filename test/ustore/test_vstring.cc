// Copyright (c) 2017 The Ustore Authors.

#include "gtest/gtest.h"

#include "spec/object_db.h"
#include "types/client/vstring.h"
#include "worker/worker.h"

const char raw_data[] = "The quick brown fox jumps over the lazy dog";

using ustore::byte_t;
using ustore::Slice;
using ustore::VMeta;
using ustore::ErrorCode;
using ustore::Hash;

ustore::Worker worker_vstring(17);
const char key_vstring[] = "key_vstring";
const char branch_vstring[] = "branch_vstring";

TEST(VString, CreateNewVString) {
  ustore::ObjectDB db(&worker_vstring);
  // create buffered new string
  ustore::VString string{Slice(raw_data)};
  // put new string
  VMeta put = db.Put(Slice(key_vstring), string, Slice(branch_vstring));
  EXPECT_TRUE(ErrorCode::kOK == put.code());
  EXPECT_TRUE(put.cell().empty());
  EXPECT_FALSE(put.version().empty());
  // get string
  VMeta get = db.Get(Slice(key_vstring), Slice(branch_vstring));
  EXPECT_TRUE(ErrorCode::kOK == get.code());
  EXPECT_FALSE(get.cell().empty());
  EXPECT_TRUE(get.version().empty());
  auto v = get.String();
  // check data
  EXPECT_EQ(0, memcmp(raw_data, v.data(), v.len()));
}
