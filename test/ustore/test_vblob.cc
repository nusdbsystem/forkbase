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

const char key_vblob[] = "key_vblob";
const char branch_vblob[] = "branch_vblob";

ustore::Worker& worker_vblob() {
  static ustore::Worker* worker = new ustore::Worker(2017);
  return *worker;
}

TEST(VBlob, CreateFromEmpty) {
  ustore::ObjectDB db(&worker_vblob());
  // create empty
  ustore::Slice empty;
  ustore::VBlob blob{Slice(empty)};
  // put new blob
  Hash hash = db.Put(Slice(key_vblob), blob, Slice(branch_vblob)).value;
  // get blob
  auto v = db.Get(Slice(key_vblob), Slice(branch_vblob)).value.Blob();
  // update blob
  std::string s = "delta";
  v.Append(reinterpret_cast<const byte_t*>(s.c_str()), s.length());
  auto update = db.Put(Slice(key_vblob), v, Slice(branch_vblob));
  EXPECT_TRUE(ErrorCode::kOK == update.stat);
  // get updated blob
  auto get = db.Get(Slice(key_vblob), Slice(branch_vblob));
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  v = get.value.Blob();
  // check data
  ustore::byte_t* buf = new ustore::byte_t[v.size()];
  v.Read(0, v.size(), buf);
  EXPECT_EQ(0, memcmp(s.data(), buf, s.length()));
  delete[] buf;

  // remove all bytes from the blob
  v = db.Get(Slice(key_vblob), Slice(branch_vblob)).value.Blob();
  // update blob
  v.Delete(0, s.length());
  update = db.Put(Slice(key_vblob), v, Slice(branch_vblob));
  EXPECT_TRUE(ErrorCode::kOK == update.stat);
  // get updated blob
  get = db.Get(Slice(key_vblob), Slice(branch_vblob));
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  v = get.value.Blob();
  // check data
  EXPECT_EQ(0, v.size());
}

TEST(VBlob, CreateNewVBlob) {
  ustore::ObjectDB db(&worker_vblob());
  // create buffered new blob
  ustore::VBlob blob{Slice(raw_data)};
  // put new blob
  auto put = db.Put(Slice(key_vblob), blob, Slice(branch_vblob));
  EXPECT_TRUE(ErrorCode::kOK == put.stat);
  // get blob
  auto get = db.Get(Slice(key_vblob), Slice(branch_vblob));
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  auto v = get.value.Blob();
  // check data
  byte_t* buf = new byte_t[v.size()];
  v.Read(0, v.size(), buf);
  EXPECT_EQ(0, memcmp(raw_data, buf, v.size()));
  delete[] buf;
}

TEST(VBlob, UpdateExistingVBlob) {
  ustore::ObjectDB db(&worker_vblob());
  // create buffered new blob
  ustore::VBlob blob{Slice(raw_data)};
  // put new blob
  Hash hash = db.Put(Slice(key_vblob), blob, Slice(branch_vblob)).value;
  // get blob
  auto v = db.Get(Slice(key_vblob), Slice(branch_vblob)).value.Blob();
  // update blob
  std::string s{raw_data};
  std::string delta = " delta";
  s += delta;
  v.Append(reinterpret_cast<const byte_t*>(s.data()+v.size()), delta.length());
  auto update = db.Put(Slice(key_vblob), v, Slice(branch_vblob));
  EXPECT_TRUE(ErrorCode::kOK == update.stat);
  // get updated blob
  auto get = db.Get(Slice(key_vblob), Slice(branch_vblob));
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  v = get.value.Blob();
  // check data
  ustore::byte_t* buf = new ustore::byte_t[v.size()];
  v.Read(0, v.size(), buf);
  EXPECT_EQ(0, memcmp(s.data(), buf, s.length()));
  delete[] buf;
}
