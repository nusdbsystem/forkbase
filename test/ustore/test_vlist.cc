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

ustore::Worker worker_vlist(17);
const char key_vlist[] = "key_vlist";
const char branch_vlist[] = "branch_vlist";

TEST(VList, CreateFromEmpty) {
  ustore::ObjectDB db(&worker_vlist);
  std::vector<Slice> slice_data;
  // create buffered new list
  ustore::VList list(slice_data);
  // put new list
  Hash hash = db.Put(Slice(key_vlist), list, Slice(branch_vlist)).version();
  // get list
  auto v = db.Get(Slice(key_vlist), Slice(branch_vlist)).List();
  // update list
  std::string delta = " delta";
  slice_data.push_back(Slice(delta));
  v.Append({Slice(delta)});
  VMeta update = db.Put(Slice(key_vlist), v, Slice(branch_vlist));
  EXPECT_TRUE(ErrorCode::kOK == update.code());
  EXPECT_TRUE(update.cell().empty());
  EXPECT_FALSE(update.version().empty());
  // get updated list
  VMeta get = db.Get(Slice(key_vlist), Slice(branch_vlist));
  EXPECT_TRUE(ErrorCode::kOK == get.code());
  EXPECT_FALSE(get.cell().empty());
  EXPECT_TRUE(get.version().empty());
  v = get.List();
  // check data

  ustore::Slice actual_val = v.Get(0);
  ASSERT_TRUE(delta == actual_val);
  ASSERT_EQ(1, v.numElements());

  // remove the only element
  v.Delete(0, 1);
  VMeta update1 = db.Put(Slice(key_vlist), v, Slice(branch_vlist));
  EXPECT_TRUE(ErrorCode::kOK == update1.code());

  VMeta get1 = db.Get(Slice(key_vlist), Slice(branch_vlist));
  EXPECT_TRUE(ErrorCode::kOK == get1.code());
  v = get1.List();
  ASSERT_EQ(0, v.numElements());
}


TEST(VList, CreateNewVList) {
  ustore::ObjectDB db(&worker_vlist);
  std::vector<Slice> slice_data;
  for (const auto& s : slist_data) slice_data.push_back(Slice(s));
  // create buffered new list
  ustore::VList list(slice_data);
  // put new list
  VMeta put = db.Put(Slice(key_vlist), list, Slice(branch_vlist));
  EXPECT_TRUE(ErrorCode::kOK == put.code());
  EXPECT_TRUE(put.cell().empty());
  EXPECT_FALSE(put.version().empty());
  // get list
  VMeta get = db.Get(Slice(key_vlist), Slice(branch_vlist));
  EXPECT_TRUE(ErrorCode::kOK == get.code());
  EXPECT_FALSE(get.cell().empty());
  EXPECT_TRUE(get.version().empty());
  auto v = get.List();
  // check data
  auto it = v.Scan();
  for (const auto& s : slice_data) {
    EXPECT_EQ(s, it.value());
    it.next();
  }
}

TEST(VList, UpdateExistingVList) {
  ustore::ObjectDB db(&worker_vlist);
  std::vector<Slice> slice_data;
  for (const auto& s : slist_data) slice_data.push_back(Slice(s));
  // create buffered new list
  ustore::VList list(slice_data);
  // put new list
  Hash hash = db.Put(Slice(key_vlist), list, Slice(branch_vlist)).version();
  // get list
  auto v = db.Get(Slice(key_vlist), Slice(branch_vlist)).List();
  // update list
  std::string delta = " delta";
  slice_data.push_back(Slice(delta));
  v.Append({Slice(delta)});
  VMeta update = db.Put(Slice(key_vlist), v, Slice(branch_vlist));
  EXPECT_TRUE(ErrorCode::kOK == update.code());
  EXPECT_TRUE(update.cell().empty());
  EXPECT_FALSE(update.version().empty());
  // get updated list
  VMeta get = db.Get(Slice(key_vlist), Slice(branch_vlist));
  EXPECT_TRUE(ErrorCode::kOK == get.code());
  EXPECT_FALSE(get.cell().empty());
  EXPECT_TRUE(get.version().empty());
  v = get.List();
  // check data
  auto it = v.Scan();
  for (const auto& s : slice_data) {
    EXPECT_EQ(s, it.value());
    it.next();
  }
}
