// Copyright (c) 2017 The Ustore Authors.

#include "gtest/gtest.h"

#include "spec/object_db.h"
#include "types/client/vset.h"
#include "worker/worker.h"

std::vector<std::string> sset_key {"The", "quick", "brown", "fox", "jumps",
                                   "over", "the", "lazy", "dog"};
std::vector<std::string> sset_val {"v1", "v2", "v3", "v4", "v5",
                                   "v6", "v7", "v8", "v9"};

using ustore::byte_t;
using ustore::Slice;
using ustore::VMeta;
using ustore::ErrorCode;
using ustore::Hash;

const char key_vset[] = "key_vset";
const char branch_vset[] = "branch_vset";
const char ctx_vset[] = "ctx_vset";

ustore::Worker& worker_vset() {
  static ustore::Worker* worker = new ustore::Worker(1994, nullptr, false);
  return *worker;
}

TEST(VSet, CreateFromEmpty) {
  ustore::ObjectDB db(&worker_vset());
  std::vector<Slice> slice_key;
  ustore::VSet set(slice_key);

  // put new set
  Hash hash = db.Put(Slice(key_vset), set, Slice(branch_vset)).value;
  // get set
  auto v = db.Get(Slice(key_vset), Slice(branch_vset)).value.Set();
  // update set
  std::string delta_key = "z delta";
  slice_key.push_back(Slice(delta_key));
  v.Set(Slice(delta_key));
  auto update = db.Put(Slice(key_vset), v, Slice(branch_vset));
  EXPECT_TRUE(ErrorCode::kOK == update.stat);
  // add to set
  auto get = db.Get(Slice(key_vset), Slice(branch_vset));
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  v = get.value.Set();

  ustore::Slice actual_val = v.Get(Slice(delta_key));
  EXPECT_TRUE(delta_key == actual_val);
  ASSERT_EQ(size_t(1), v.numElements());

  // remove the only key
  v.Remove(Slice(delta_key));
  auto update1 = db.Put(Slice(key_vset), v, Slice(branch_vset));
  EXPECT_TRUE(ErrorCode::kOK == update1.stat);

  auto get1 = db.Get(Slice(key_vset), Slice(branch_vset));
  EXPECT_TRUE(ErrorCode::kOK == get1.stat);
  v = get1.value.Set();
  ASSERT_EQ(size_t(0), v.numElements());
}

TEST(VSet, CreateNewVset) {
  ustore::ObjectDB db(&worker_vset());
  std::sort(sset_key.begin(), sset_key.end());
  std::vector<Slice> slice_key;
  for (const auto& s : sset_key) slice_key.push_back(Slice(s));
  // create buffered new set
  ustore::VSet set(slice_key);
  set.SetContext(Slice(ctx_vset));
  // put new set
  auto put = db.Put(Slice(key_vset), set, Slice(branch_vset));
  EXPECT_TRUE(ErrorCode::kOK == put.stat);
  // get set
  auto get = db.Get(Slice(key_vset), Slice(branch_vset));
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  // check context
  EXPECT_EQ(Slice(ctx_vset), get.value.cell().context());
  auto v = get.value.Set();
  // check data
  auto it = v.Scan();
  for (size_t i = 0; i < slice_key.size(); ++i) {
    EXPECT_EQ(slice_key[i], it.key());
    it.next();
  }
}

TEST(VSet, CreateUnkeyedVset) {
  ustore::ObjectDB db(&worker_vset());
  std::sort(sset_key.begin(), sset_key.end());
  std::vector<Slice> slice_key;
  for (const auto& s : sset_key) slice_key.push_back(Slice(s));
  // create buffered new set
  ustore::VSet set(slice_key);
  // put new set
  auto put = db.PutUnkeyed(Slice(key_vset), set);
  EXPECT_TRUE(ErrorCode::kOK == put.stat);
  // get set
  auto get = db.GetUnkeyed(Slice(key_vset), ustore::UType::kSet, put.value);
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  auto v = get.value.Set();
  // check data
  auto it = v.Scan();
  for (size_t i = 0; i < slice_key.size(); ++i) {
    EXPECT_EQ(slice_key[i], it.key());
    it.next();
  }
}

TEST(VSet, AddToExistingVset) {
  ustore::ObjectDB db(&worker_vset());
  std::sort(sset_key.begin(), sset_key.end());
  std::vector<Slice> slice_key;
  for (const auto& s : sset_key) slice_key.push_back(Slice(s));
  // create buffered new set
  ustore::VSet set(slice_key);
  // put new set
  Hash hash = db.Put(Slice(key_vset), set, Slice(branch_vset)).value;
  // get set
  auto v = db.Get(Slice(key_vset), Slice(branch_vset)).value.Set();
  // update set
  std::string delta_key = "z delta";
  slice_key.push_back(Slice(delta_key));
  v.Set(Slice(delta_key));
  auto update = db.Put(Slice(key_vset), v, Slice(branch_vset));
  EXPECT_TRUE(ErrorCode::kOK == update.stat);
  // add to set
  auto get = db.Get(Slice(key_vset), Slice(branch_vset));
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  v = get.value.Set();
  // check data
  auto it = v.Scan();
  for (size_t i = 0; i < slice_key.size(); ++i) {
    EXPECT_EQ(slice_key[i], it.key());
    it.next();
  }
}

TEST(VSet, RemoveFromExistingVset) {
  ustore::ObjectDB db(&worker_vset());
  std::sort(sset_key.begin(), sset_key.end());
  std::vector<Slice> slice_key;
  for (const auto& s : sset_key) slice_key.push_back(Slice(s));
  // create buffered new set
  ustore::VSet set(slice_key);
  // put new set
  Hash hash = db.Put(Slice(key_vset), set, Slice(branch_vset)).value;
  // get set
  auto v = db.Get(Slice(key_vset), Slice(branch_vset)).value.Set();
  // remove from set
  v.Remove(slice_key[slice_key.size()-1]);
  auto update = db.Put(Slice(key_vset), v, Slice(branch_vset));
  EXPECT_TRUE(ErrorCode::kOK == update.stat);
  // get updated set
  auto get = db.Get(Slice(key_vset), Slice(branch_vset));
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  v = get.value.Set();
  // check data
  auto it = v.Scan();
  for (size_t i = 0; i < slice_key.size() - 1; ++i) {
    EXPECT_EQ(slice_key[i], it.key());
    it.next();
  }
}

TEST(VSet, DestructWorker) {
  delete &worker_vset();
}
