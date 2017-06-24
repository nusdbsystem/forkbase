// Copyright (c) 2017 The Ustore Authors.

#include "gtest/gtest.h"

#include "spec/object_db.h"
#include "types/client/vlist.h"
#include "worker/worker.h"

std::vector<std::string> slist_data {"The", "quick", "brown", "fox", "jumps",
                                     "over", "the", "lazy", "dog"};

using ustore::byte_t;
using ustore::Slice;
using ustore::VMeta;
using ustore::ErrorCode;
using ustore::Hash;

const char key_vlist[] = "key_vlist";
const char branch_vlist[] = "branch_vlist";

ustore::Worker& worker_vlist() {
  static ustore::Worker* worker = new ustore::Worker(2017, false);
  return *worker;
}

TEST(VList, CreateFromEmpty) {
  ustore::ObjectDB db(&worker_vlist());
  std::vector<Slice> slice_data;
  // create buffered new list
  ustore::VList list(slice_data);
  // put new list
  Hash hash = db.Put(Slice(key_vlist), list, Slice(branch_vlist)).value;
  // get list
  auto v = db.Get(Slice(key_vlist), Slice(branch_vlist)).value.List();
  // update list
  std::string delta = " delta";
  slice_data.push_back(Slice(delta));
  v.Append({Slice(delta)});
  auto update = db.Put(Slice(key_vlist), v, Slice(branch_vlist));
  EXPECT_TRUE(ErrorCode::kOK == update.stat);
  // get updated list
  auto get = db.Get(Slice(key_vlist), Slice(branch_vlist));
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  v = get.value.List();
  // check data

  ustore::Slice actual_val = v.Get(0);
  ASSERT_TRUE(delta == actual_val);
  ASSERT_EQ(size_t(1), v.numElements());

  // remove the only element
  v.Delete(0, 1);
  auto update1 = db.Put(Slice(key_vlist), v, Slice(branch_vlist));
  EXPECT_TRUE(ErrorCode::kOK == update1.stat);

  auto get1 = db.Get(Slice(key_vlist), Slice(branch_vlist));
  EXPECT_TRUE(ErrorCode::kOK == get1.stat);
  v = get1.value.List();
  ASSERT_EQ(size_t(0), v.numElements());
}


TEST(VList, CreateNewVList) {
  ustore::ObjectDB db(&worker_vlist());
  std::vector<Slice> slice_data;
  for (const auto& s : slist_data) slice_data.push_back(Slice(s));
  // create buffered new list
  ustore::VList list(slice_data);
  // put new list
  auto put = db.Put(Slice(key_vlist), list, Slice(branch_vlist));
  EXPECT_TRUE(ErrorCode::kOK == put.stat);
  // get list
  auto get = db.Get(Slice(key_vlist), Slice(branch_vlist));
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  auto v = get.value.List();
  // check data
  auto it = v.Scan();
  for (const auto& s : slice_data) {
    EXPECT_EQ(s, it.value());
    it.next();
  }
}

TEST(VList, UpdateExistingVList) {
  ustore::ObjectDB db(&worker_vlist());
  std::vector<Slice> slice_data;
  for (const auto& s : slist_data) slice_data.push_back(Slice(s));
  // create buffered new list
  ustore::VList list(slice_data);
  // put new list
  Hash hash = db.Put(Slice(key_vlist), list, Slice(branch_vlist)).value;
  // get list
  auto v = db.Get(Slice(key_vlist), Slice(branch_vlist)).value.List();
  // update list
  std::string delta = " delta";
  slice_data.push_back(Slice(delta));
  v.Append({Slice(delta)});
  auto update = db.Put(Slice(key_vlist), v, Slice(branch_vlist));
  EXPECT_TRUE(ErrorCode::kOK == update.stat);
  // get updated list
  auto get = db.Get(Slice(key_vlist), Slice(branch_vlist));
  EXPECT_TRUE(ErrorCode::kOK == get.stat);
  v = get.value.List();
  // check data
  auto it = v.Scan();
  for (const auto& s : slice_data) {
    EXPECT_EQ(s, it.value());
    it.next();
  }
}
