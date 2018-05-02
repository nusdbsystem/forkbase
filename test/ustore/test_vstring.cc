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

const char key_vstring[] = "key_vstring";
const char branch_vstring[] = "branch_vstring";
const char ctx_vstring[] = "ctx_vstring";

ustore::Worker& worker_vstring() {
  static ustore::Worker* worker = new ustore::Worker(1995, nullptr, false);
  return *worker;
}

TEST(VString, CreateFromEmpty) {
  ustore::ObjectDB db(&worker_vstring());
  Slice empty;
  ustore::VString string(empty);

  auto put = db.Put(Slice(key_vstring), string, Slice(branch_vstring));
  EXPECT_TRUE(ErrorCode::kOK == put.stat);
  // get string
  auto get = db.Get(Slice(key_vstring), Slice(branch_vstring));
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  auto v = get.value.String();

  // check data
  EXPECT_EQ(size_t(0), v.len());
  EXPECT_EQ(nullptr, v.data());
}


TEST(VString, CreateNewVString) {
  ustore::ObjectDB db(&worker_vstring());
  // create buffered new string
  ustore::VString string{Slice(raw_data)};
  string.SetContext(Slice(ctx_vstring));
  // put new string
  auto put = db.Put(Slice(key_vstring), string, Slice(branch_vstring));
  EXPECT_TRUE(ErrorCode::kOK == put.stat);
  // get string
  auto get = db.Get(Slice(key_vstring), Slice(branch_vstring));
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  // check context
  EXPECT_EQ(Slice(ctx_vstring), get.value.cell().context());
  auto v = get.value.String();
  // check data
  EXPECT_EQ(0, memcmp(raw_data, v.data(), v.len()));
}

TEST(VString, DestructWorker) {
  delete &worker_vstring();
}
