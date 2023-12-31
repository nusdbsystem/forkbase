// Copyright (c) 2017 The Ustore Authors.

#include "gtest/gtest.h"

#include "spec/object_db.h"
#include "types/client/vmap.h"
#include "worker/worker.h"

std::vector<std::string> smap_key {"The", "quick", "brown", "fox", "jumps",
  "over", "the", "lazy", "dog"};
std::vector<std::string> smap_val {"v1", "v2", "v3", "v4", "v5",
  "v6", "v7", "v8", "v9"};

using ustore::byte_t;
using ustore::Slice;
using ustore::VMeta;
using ustore::ErrorCode;
using ustore::Hash;

const char key_vmap[] = "key_vmap";
const char branch_vmap[] = "branch_vmap";
const char ctx_vmap[] = "ctx_vmap";

ustore::Worker& worker_vmap() {
  static ustore::Worker* worker = new ustore::Worker(1993, nullptr, false);
  return *worker;
}

TEST(VMap, CreateFromEmpty) {
  ustore::ObjectDB db(&worker_vmap());
  std::vector<Slice> slice_key, slice_val;
  ustore::VMap map(slice_key, slice_val);

  // put new map
  Hash hash = db.Put(Slice(key_vmap), map, Slice(branch_vmap)).value;
  // get map
  auto v = db.Get(Slice(key_vmap), Slice(branch_vmap)).value.Map();
  // update map
  std::string delta_key = "z delta";
  std::string delta_val = "v delta";
  slice_key.push_back(Slice(delta_key));
  slice_val.push_back(Slice(delta_val));
  v.Set(Slice(delta_key), Slice(delta_val));
  auto update = db.Put(Slice(key_vmap), v, Slice(branch_vmap));
  EXPECT_TRUE(ErrorCode::kOK == update.stat);
  // add to map
  auto get = db.Get(Slice(key_vmap), Slice(branch_vmap));
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  v = get.value.Map();

  ustore::Slice actual_val = v.Get(Slice(delta_key));
  ASSERT_TRUE(delta_val == actual_val);
  ASSERT_EQ(size_t(1), v.numElements());

  // remove the only key
  v.Remove(Slice(delta_key));
  auto update1 = db.Put(Slice(key_vmap), v, Slice(branch_vmap));
  EXPECT_TRUE(ErrorCode::kOK == update1.stat);

  auto get1 = db.Get(Slice(key_vmap), Slice(branch_vmap));
  EXPECT_TRUE(ErrorCode::kOK == get1.stat);
  v = get1.value.Map();
  ASSERT_EQ(size_t(0), v.numElements());
}

TEST(VMap, CreateNewVMap) {
  ustore::ObjectDB db(&worker_vmap());
  std::sort(smap_key.begin(), smap_key.end());
  std::vector<Slice> slice_key, slice_val;
  for (const auto& s : smap_key) slice_key.push_back(Slice(s));
  for (const auto& s : smap_val) slice_val.push_back(Slice(s));
  // create buffered new map
  ustore::VMap map(slice_key, slice_val);
  map.SetContext(Slice(ctx_vmap));
  // put new map
  auto put = db.Put(Slice(key_vmap), map, Slice(branch_vmap));
  EXPECT_TRUE(ErrorCode::kOK == put.stat);
  // get map
  auto get = db.Get(Slice(key_vmap), Slice(branch_vmap));
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  // check context
  EXPECT_EQ(Slice(ctx_vmap), get.value.cell().context());
  auto v = get.value.Map();
  // check data
  auto it = v.Scan();
  for (size_t i = 0; i < slice_key.size(); ++i) {
    EXPECT_EQ(slice_key[i], it.key());
    EXPECT_EQ(slice_val[i], it.value());
    it.next();
  }
}

TEST(VMap, CreateUnkeyedVMap) {
  ustore::ObjectDB db(&worker_vmap());
  std::sort(smap_key.begin(), smap_key.end());
  std::vector<Slice> slice_key, slice_val;
  for (const auto& s : smap_key) slice_key.push_back(Slice(s));
  for (const auto& s : smap_val) slice_val.push_back(Slice(s));
  // create buffered new map
  ustore::VMap map(slice_key, slice_val);
  // put new map
  auto put = db.PutUnkeyed(Slice(key_vmap), map);
  EXPECT_TRUE(ErrorCode::kOK == put.stat);
  // get map
  auto get = db.GetUnkeyed(Slice(key_vmap), ustore::UType::kMap, put.value);
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  auto v = get.value.Map();
  // check data
  auto it = v.Scan();
  for (size_t i = 0; i < slice_key.size(); ++i) {
    EXPECT_EQ(slice_key[i], it.key());
    EXPECT_EQ(slice_val[i], it.value());
    it.next();
  }
}

TEST(VMap, AddToExistingVMap) {
  ustore::ObjectDB db(&worker_vmap());
  std::sort(smap_key.begin(), smap_key.end());
  std::vector<Slice> slice_key, slice_val;
  for (const auto& s : smap_key) slice_key.push_back(Slice(s));
  for (const auto& s : smap_val) slice_val.push_back(Slice(s));
  // create buffered new map
  ustore::VMap map(slice_key, slice_val);
  // put new map
  Hash hash = db.Put(Slice(key_vmap), map, Slice(branch_vmap)).value;
  // get map
  auto v = db.Get(Slice(key_vmap), Slice(branch_vmap)).value.Map();
  // update map
  std::string delta_key = "z delta";
  std::string delta_val = "v delta";
  slice_key.push_back(Slice(delta_key));
  slice_val.push_back(Slice(delta_val));
  v.Set(Slice(delta_key), Slice(delta_val));
  auto update = db.Put(Slice(key_vmap), v, Slice(branch_vmap));
  EXPECT_TRUE(ErrorCode::kOK == update.stat);
  // add to map
  auto get = db.Get(Slice(key_vmap), Slice(branch_vmap));
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  v = get.value.Map();
  // check data
  auto it = v.Scan();
  for (size_t i = 0; i < slice_key.size(); ++i) {
    EXPECT_EQ(slice_key[i], it.key());
    EXPECT_EQ(slice_val[i], it.value());
    it.next();
  }
}

TEST(VMap, RemoveFromExistingVMap) {
  ustore::ObjectDB db(&worker_vmap());
  std::sort(smap_key.begin(), smap_key.end());
  std::vector<Slice> slice_key, slice_val;
  for (const auto& s : smap_key) slice_key.push_back(Slice(s));
  for (const auto& s : smap_val) slice_val.push_back(Slice(s));
  // create buffered new map
  ustore::VMap map(slice_key, slice_val);
  // put new map
  Hash hash = db.Put(Slice(key_vmap), map, Slice(branch_vmap)).value;
  // get map
  auto v = db.Get(Slice(key_vmap), Slice(branch_vmap)).value.Map();
  // remove from map
  v.Remove(slice_key[slice_key.size() - 1]);
  auto update = db.Put(Slice(key_vmap), v, Slice(branch_vmap));
  EXPECT_TRUE(ErrorCode::kOK == update.stat);
  // get updated map
  auto get = db.Get(Slice(key_vmap), Slice(branch_vmap));
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  v = get.value.Map();
  // check data
  auto it = v.Scan();
  for (size_t i = 0; i < slice_key.size() - 1; ++i) {
    EXPECT_EQ(slice_key[i], it.key());
    EXPECT_EQ(slice_val[i], it.value());
    it.next();
  }
}

TEST(VMap, UpdateExistingVMap) {
  ustore::ObjectDB db(&worker_vmap());
  std::vector<Slice> slice_key, slice_val;
  for (const auto& s : smap_key) slice_key.push_back(Slice(s));
  for (const auto& s : smap_val) slice_val.push_back(Slice(s));
  // create buffered new map
  ustore::VMap map(slice_key, slice_val);
  // put new map
  Hash hash = db.Put(Slice(key_vmap), map, Slice(branch_vmap)).value;
  // get map
  auto v = db.Get(Slice(key_vmap), Slice(branch_vmap)).value.Map();
  // update map
  std::string nv = "new_v";
  v.Set(slice_key[0], Slice(nv));
  slice_val[0] = Slice(nv);
  auto update = db.Put(Slice(key_vmap), v, Slice(branch_vmap));
  EXPECT_TRUE(ErrorCode::kOK == update.stat);
  // get updated map
  auto get = db.Get(Slice(key_vmap), Slice(branch_vmap));
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  v = get.value.Map();
  // check data
  auto it = v.Scan();
  for (size_t i = 0; i < slice_key.size(); ++i) {
    EXPECT_EQ(slice_key[i], it.key());
    EXPECT_EQ(slice_val[i], it.value());
    it.next();
  }
}

TEST(VMap, MultiSetExistingVMap) {
  // Init Map with a single entry
  ustore::ObjectDB db(&worker_vmap());
  std::vector<Slice> slice_key, slice_val;
  for (const auto& s : smap_key) slice_key.push_back(Slice(s));
  for (const auto& s : smap_val) slice_val.push_back(Slice(s));

  ustore::VMap map(slice_key, slice_val);
  Hash hash = db.Put(Slice(key_vmap), map, Slice(branch_vmap)).value;

  // get map
  auto v = db.Get(Slice(key_vmap), Slice(branch_vmap)).value.Map();

  // Multiset map
  v.Set(slice_key, slice_val);
  auto update = db.Put(Slice(key_vmap), v, Slice(branch_vmap));
  ASSERT_TRUE(ErrorCode::kOK == update.stat);

  auto get = db.Get(Slice(key_vmap), Slice(branch_vmap));
  ASSERT_TRUE(ErrorCode::kOK == get.stat);
  v = get.value.Map();
  // check data
  auto it = v.Scan();
  for (size_t i = 0; i < slice_key.size(); ++i) {
    EXPECT_EQ(slice_key[i], it.key());
    EXPECT_EQ(slice_val[i], it.value());
    it.next();
  }
}

TEST(VMap, MultiSetExistingVMapFromStdMap) {
  // Init Map with a single entry
  ustore::ObjectDB db(&worker_vmap());
  std::vector<Slice> slice_key, slice_val;
  for (const auto& s : smap_key) slice_key.push_back(Slice(s));
  for (const auto& s : smap_val) slice_val.push_back(Slice(s));

  ustore::VMap map(slice_key, slice_val);
  Hash hash = db.Put(Slice(key_vmap), map, Slice(branch_vmap)).value;

  // get map
  auto v = db.Get(Slice(key_vmap), Slice(branch_vmap)).value.Map();

  // Multiset map
  std::map<std::string, std::string> kvs;
  for (size_t i = 0; i < smap_key.size(); ++i) kvs[smap_key[i]] = smap_val[i];
  v.Insert(kvs);
  auto update = db.Put(Slice(key_vmap), v, Slice(branch_vmap));
  ASSERT_TRUE(ErrorCode::kOK == update.stat);

  auto get = db.Get(Slice(key_vmap), Slice(branch_vmap));
  ASSERT_TRUE(ErrorCode::kOK == get.stat);
  v = get.value.Map();
  // check data
  auto it = v.Scan();
  for (size_t i = 0; i < slice_key.size(); ++i) {
    EXPECT_EQ(slice_key[i], it.key());
    EXPECT_EQ(slice_val[i], it.value());
    it.next();
  }
}

TEST(VMap, DestructWorker) {
  delete &worker_vmap();
}
