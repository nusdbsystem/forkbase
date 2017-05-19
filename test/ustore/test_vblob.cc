// Copyright (c) 2017 The Ustore Authors.

#include "gtest/gtest.h"

#include "spec/object_db.h"
#include "types/client/vblob.h"
#include "worker/worker.h"

const char raw_data[] = "The quick brown fox jumps over the lazy dog";

using ustore::byte_t;
using ustore::Slice;
using ustore::VMeta;
using ustore::ErrorCode;
using ustore::Hash;

ustore::Worker worker_vblob(17);
const char key_vblob[] = "key_vblob";
const char branch_vblob[] = "branch_vblob";

TEST(VBlob, CreateNewVBlob) {
  ustore::ObjectDB db(&worker_vblob);
  // create buffered new blob
  ustore::VBlob blob{Slice(raw_data)};
  // put new blob
  VMeta put = db.Put(Slice(key_vblob), blob, Slice(branch_vblob));
  EXPECT_TRUE(ErrorCode::kOK == put.code());
  EXPECT_TRUE(put.cell().empty());
  EXPECT_FALSE(put.hash().empty());
  // get blob
  VMeta get = db.Get(Slice(key_vblob), Slice(branch_vblob));
  EXPECT_TRUE(ErrorCode::kOK == get.code());
  EXPECT_FALSE(get.cell().empty());
  EXPECT_TRUE(get.hash().empty());
  auto v = get.Blob();
  // check data
  byte_t* buf = new byte_t[v.size()];
  v.Read(0, v.size(), buf);
  EXPECT_EQ(0, memcmp(raw_data, buf, v.size()));
  delete[] buf;
}

TEST(VBlob, UpdateExistingVBlob) {
  ustore::ObjectDB db(&worker_vblob);
  // create buffered new blob
  ustore::VBlob blob{Slice(raw_data)};
  // put new blob
  Hash hash = db.Put(Slice(key_vblob), blob, Slice(branch_vblob)).hash();
  // get blob
  auto v = db.Get(Slice(key_vblob), Slice(branch_vblob)).Blob();
  // update blob
  std::string s{raw_data};
  std::string delta = " delta";
  s += delta;
  v.Append(reinterpret_cast<const byte_t*>(s.data()+v.size()), delta.length());
  VMeta update = db.Put(Slice(key_vblob), v, Slice(branch_vblob));
  EXPECT_TRUE(ErrorCode::kOK == update.code());
  EXPECT_TRUE(update.cell().empty());
  EXPECT_FALSE(update.hash().empty());
  // get updated blob
  VMeta get = db.Get(Slice(key_vblob), Slice(branch_vblob));
  EXPECT_TRUE(ErrorCode::kOK == get.code());
  EXPECT_FALSE(get.cell().empty());
  EXPECT_TRUE(get.hash().empty());
  v = get.Blob();
  // check data
  ustore::byte_t* buf = new ustore::byte_t[v.size()];
  v.Read(0, v.size(), buf);
  EXPECT_EQ(0, memcmp(s.data(), buf, s.length()));
  delete[] buf;
}
