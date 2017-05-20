// Copyright (c) 2017 The Ustore Authors.

#include <forward_list>
#include <utility>
#include <vector>
#include "gtest/gtest.h"
#include "hash/hash.h"
#include "spec/slice.h"
#include "spec/value.h"
#include "types/type.h"
#include "utils/utils.h"
#include "worker/worker_ext.h"

using namespace ustore;
using SliceFwdList = std::forward_list<Slice>;
using ValueVec = std::vector<Value>;
using HashVec = std::vector<Hash>;

const Slice key[] = {
  Slice("KeyZero"), Slice("KeyOne"), Slice("KeyTwo"), Slice("KeyThree"),
  Slice("KeyFour"), Slice("KeyFive")
};

const Slice branch[] = {
  Slice("BranchMaster"), Slice("BranchFirst"), Slice("BranchSecond"),
  Slice("BranchThird"), Slice("BranchFourth")
};

const SliceFwdList slices {
  Slice("The quick brown fox jumps over the lazy dog"),  //-- 0
  Slice("Edge of tomorrow"),  //----------------------------- 1
  Slice("Pig can fly!"),  //--------------------------------- 2
  Slice("Have you ever seen the rain?"),  //----------------- 3
  Slice("Once upon a time"),  //----------------------------- 4
  Slice("Good good study, day day up!"),  //----------------- 5
  Slice("I am a hero"),  //---------------------------------- 6
  Slice("Mad detector"),  //--------------------------------- 7
  Slice("Stay hungry. Stay foolish."),  //------------------- 8
  Slice("To be, or not to be, that is the question."),  //--- 9
  Slice("What goes around, comes around")  //---------------- 10
};

template<class T>
const ValueVec ToValues(
  const std::function<const T(const Slice&)>f_slice2val) {
  ValueVec val;
  for (const auto& s : slices) val.emplace_back(f_slice2val(s));
  return val;
}

const ValueVec val_str(ToValues<Slice>([](const Slice& s) {
  return s;
}));

const ValueVec val_blob(ToValues<Blob>([](const Slice& s) {
  return Blob(s.data(), s.len());
}));

HashVec ver;
ErrorCode ec{ErrorCode::kUnknownOp};

const WorkerID worker_id{14};
WorkerExt worker{worker_id};

TEST(Worker, Meta) {
  EXPECT_EQ(worker_id, worker.id());
}

TEST(Worker, NamedBranch_GetPutString) {
  Hash version;

  EXPECT_EQ(0, worker.ListBranch(key[0]).size());
  EXPECT_FALSE(worker.Exists(key[0]));

  ec = worker.Put(key[0], val_str[0], branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[0]
  EXPECT_TRUE(worker.Exists(key[0]));
  EXPECT_EQ(1, worker.ListBranch(key[0]).size());
  EXPECT_EQ(ver[0], worker.GetBranchHead(key[0], branch[0]));
  EXPECT_TRUE(worker.IsLatest(key[0], ver[0]));

  ec = worker.Put(key[0], val_str[1], branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[1]
  EXPECT_FALSE(worker.IsBranchHead(key[0], branch[0], ver[0]));
  EXPECT_TRUE(worker.IsLatest(key[0], ver[1]));

  ec = worker.Put(key[0], val_str[2], branch[1], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[2]
  EXPECT_EQ(2, worker.ListBranch(key[0]).size());

  Value value;

  ec = worker.Get(key[0], ver[0], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(val_str[0], value);

  ec = worker.Get(key[0], branch[0], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(val_str[1], value);

  ec = worker.Get(key[0], branch[1], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(val_str[2], value);
  EXPECT_EQ(ver[2], worker.GetBranchHead(key[0], branch[1]));
}

TEST(Worker, NamedBranch_Branch) {
  ec = worker.Branch(key[0], branch[0], branch[2]);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(3, worker.ListBranch(key[0]).size());
  EXPECT_EQ(worker.GetBranchHead(key[0], branch[0]),
            worker.GetBranchHead(key[0], branch[2]));
  EXPECT_EQ(2, worker.GetLatestVersions(key[0]).size());
}

TEST(Worker, NamedBranch_Rename) {
  const auto version = worker.GetBranchHead(key[0], branch[0]);
  EXPECT_EQ(ver[1], version);
  ec = worker.Rename(key[0], branch[0], branch[3]);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(version, worker.GetBranchHead(key[0], branch[3]));
  EXPECT_EQ(3, worker.ListBranch(key[0]).size());
  EXPECT_FALSE(worker.Exists(key[0], branch[0]));
  EXPECT_TRUE(worker.Exists(key[0], branch[3]));
  EXPECT_EQ(2, worker.GetLatestVersions(key[0]).size());
}

TEST(Worker, NamedBranch_Merge) {
  Hash version;

  worker.Merge(key[0], val_str[3], branch[3], branch[1], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[3]
  EXPECT_TRUE(worker.IsBranchHead(key[0], branch[3], ver[3]));
  EXPECT_FALSE(worker.IsBranchHead(key[0], branch[1], ver[3]));
  EXPECT_EQ(3, worker.ListBranch(key[0]).size());
  EXPECT_EQ(1, worker.GetLatestVersions(key[0]).size());

  worker.Merge(key[0], val_str[4], branch[1], ver[1], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[4]
  EXPECT_EQ(2, worker.GetLatestVersions(key[0]).size());

  Value value;

  ec = worker.Get(key[0], branch[3], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(val_str[3], value);

  ec = worker.Get(key[0], branch[1], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(val_str[4], value);
}

TEST(Worker, UnnamedBranch_GetPutBlob) {
  Hash version;

  worker.Put(key[1], val_blob[5], Hash::kNull, &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[5]
  EXPECT_EQ(1, worker.GetLatestVersions(key[1]).size());

  worker.Put(key[1], val_blob[6], ver[5], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[6]
  EXPECT_EQ(1, worker.GetLatestVersions(key[1]).size());
  EXPECT_FALSE(worker.IsLatest(key[1], ver[5]));
  EXPECT_TRUE(worker.IsLatest(key[1], ver[6]));

  worker.Put(key[1], val_blob[7], ver[5], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[7]
  EXPECT_EQ(2, worker.GetLatestVersions(key[1]).size());

  worker.Put(key[1], val_blob[8], ver[7], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[8]
  EXPECT_EQ(2, worker.GetLatestVersions(key[1]).size());

  Value value;

  worker.Get(key[1], ver[5], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(val_blob[5], value);

  worker.Get(key[1], ver[6], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(val_blob[6], value);

  worker.Get(key[1], ver[7], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(val_blob[7], value);

  worker.Get(key[1], ver[8], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(val_blob[8], value);
}

TEST(Worker, UnnamedBranch_Merge) {
  Hash version;

  worker.Merge(key[1], val_blob[9], ver[6], ver[7], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[9]
  EXPECT_EQ(2, worker.GetLatestVersions(key[1]).size());

  worker.Merge(key[1], val_blob[10], ver[8], ver[9], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[10]
  EXPECT_EQ(1, worker.GetLatestVersions(key[1]).size());

  Value value;

  worker.Get(key[1], ver[9], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(val_blob[9], value);

  worker.Get(key[1], ver[10], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(val_blob[10], value);
}

TEST(Worker, NamedBranch_GetPutString_Value2) {
  Hash version;

  Value2 val2;
  val2.type = UType::kString;
  val2.base = Hash::kNull;
  EXPECT_EQ(0, val2.vals.size());
  val2.vals.emplace_back(val_str.back().slice());

  EXPECT_EQ(0, worker.ListBranch(key[2]).size());

  val2.vals[0] = val_str[0].slice();
  ec = worker.Put(key[2], val2, branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[11]
  EXPECT_EQ(1, worker.ListBranch(key[2]).size());
  EXPECT_EQ(ver[11], worker.GetBranchHead(key[2], branch[0]));
  EXPECT_TRUE(worker.IsLatest(key[2], ver[11]));

  val2.vals[0] = val_str[1].slice();
  ec = worker.Put(key[2], val2, branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[12]
  EXPECT_FALSE(worker.IsBranchHead(key[2], branch[0], ver[11]));
  EXPECT_TRUE(worker.IsLatest(key[2], ver[12]));

  val2.vals[0] = val_str[2].slice();
  ec = worker.Put(key[2], val2, branch[1], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[13]
  EXPECT_EQ(2, worker.ListBranch(key[2]).size());

  Value value;

  ec = worker.Get(key[2], ver[11], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(val_str[0], value);

  ec = worker.Get(key[2], branch[0], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(val_str[1], value);

  ec = worker.Get(key[2], branch[1], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(val_str[2], value);
  EXPECT_EQ(ver[13], worker.GetBranchHead(key[2], branch[1]));
}

TEST(Worker, NamedBranch_GetPutList_Value2) {
  Hash version;
  UCell ucell;

  Value2 val;
  val.type = UType::kList;
  EXPECT_EQ(0, val.vals.size());

  EXPECT_EQ(0, worker.ListBranch(key[3]).size());

  // construct a new list
  val.base = Hash::kNull;
  val.vals.emplace_back(val_str[0].slice());
  val.vals.emplace_back(val_str[1].slice());
  val.vals.emplace_back(val_str[2].slice());

  ec = worker.Put(key[3], val, branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[14]
  EXPECT_EQ(1, worker.ListBranch(key[3]).size());
  EXPECT_EQ(ver[14], worker.GetBranchHead(key[3], branch[0]));
  EXPECT_TRUE(worker.IsLatest(key[3], ver[14]));

  ec = worker.GetForType(UType::kList, key[3], branch[0], &ucell);
  EXPECT_EQ(ErrorCode::kOK, ec);
  const SList list1(ucell.dataHash());
  auto itr_list1 = list1.Scan();
  EXPECT_EQ(val_str[0].slice(), itr_list1->value());
  EXPECT_TRUE(itr_list1->next());
  EXPECT_EQ(val_str[1].slice(), itr_list1->value());
  EXPECT_TRUE(itr_list1->next());
  EXPECT_FALSE(itr_list1->next());
  EXPECT_EQ(val_str[2].slice(), list1.Get(2));

  // update existing list by replacing one element
  val.base = list1.hash();
  val.pos = 1;
  val.dels = 1;
  val.vals.clear();
  val.vals.emplace_back(val_str[3].slice());

  ec = worker.Put(key[3], val, branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[15]
  EXPECT_EQ(1, worker.ListBranch(key[3]).size());
  EXPECT_FALSE(worker.IsLatest(key[3], ver[14]));
  EXPECT_EQ(ver[15], worker.GetBranchHead(key[3], branch[0]));

  ec = worker.GetForType(UType::kList, key[3], branch[0], &ucell);
  EXPECT_EQ(ErrorCode::kOK, ec);
  const SList list2(ucell.dataHash());
  EXPECT_EQ(3, list2.numElements());
  EXPECT_EQ(val_str[0].slice(), list2.Get(0));
  EXPECT_EQ(val_str[3].slice(), list2.Get(1));
  EXPECT_EQ(val_str[2].slice(), list2.Get(2));

  // update existing list by inserting new elements
  val.base = list2.hash();
  val.pos = 1;
  val.dels = 0;
  val.vals.clear();
  val.vals.emplace_back(val_str[4].slice());
  val.vals.emplace_back(val_str[5].slice());

  ec = worker.Put(key[3], val, branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[16]

  // diff on two lists
  ec = worker.GetForType(UType::kList, key[3], branch[0], &ucell);
  EXPECT_EQ(ErrorCode::kOK, ec);
  const SList list3(ucell.dataHash());
  EXPECT_EQ(5, list3.numElements());
  auto itr_diff = UList::DuallyDiff(list1, list3);
  EXPECT_EQ(1, itr_diff->index());
  EXPECT_EQ(val_str[1].slice(), itr_diff->lhs_value());
  EXPECT_EQ(val_str[4].slice(), itr_diff->rhs_value());
  EXPECT_TRUE(itr_diff->next());
  EXPECT_EQ(2, itr_diff->index());
  EXPECT_EQ(val_str[2].slice(), itr_diff->lhs_value());
  EXPECT_EQ(val_str[5].slice(), itr_diff->rhs_value());
  EXPECT_TRUE(itr_diff->next());
  EXPECT_EQ(3, itr_diff->index());
  EXPECT_TRUE(itr_diff->lhs_value().empty());
  EXPECT_EQ(val_str[3].slice(), itr_diff->rhs_value());
  EXPECT_TRUE(itr_diff->next());
  EXPECT_EQ(4, itr_diff->index());
  EXPECT_TRUE(itr_diff->lhs_value().empty());
  EXPECT_EQ(val_str[2].slice(), itr_diff->rhs_value());
  EXPECT_FALSE(itr_diff->next());
}

TEST(Worker, NamedBranch_GetPutMap_Value2) {
  Hash version;
  UCell ucell;

  Value2 val;
  val.type = UType::kMap;
  EXPECT_EQ(0, val.keys.size());
  EXPECT_EQ(0, val.vals.size());

  EXPECT_EQ(0, worker.ListBranch(key[4]).size());

  // construct a new map
  val.base = Hash::kNull;
  val.keys.clear();
  val.keys.emplace_back(val_str[0].slice());
  val.keys.emplace_back(val_str[1].slice());
  val.vals.clear();
  val.vals.emplace_back(val_str[5].slice());
  val.vals.emplace_back(val_str[6].slice());

  ec = worker.Put(key[4], val, branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[17]
  EXPECT_EQ(1, worker.ListBranch(key[4]).size());
  EXPECT_EQ(ver[17], worker.GetBranchHead(key[4], branch[0]));
  EXPECT_TRUE(worker.IsLatest(key[4], ver[17]));

  ec = worker.GetForType(UType::kMap, key[4], branch[0], &ucell);
  EXPECT_EQ(ErrorCode::kOK, ec);
  const SMap map1(ucell.dataHash());
  EXPECT_EQ(val_str[5].slice(), map1.Get(val_str[0].slice()));
  EXPECT_EQ(val_str[6].slice(), map1.Get(val_str[1].slice()));

  // update an entry in the map
  val.base = map1.hash();
  val.keys.clear();
  val.keys.emplace_back(val_str[2].slice());
  val.vals.clear();
  val.vals.emplace_back(val_str[7].slice());

  ec = worker.Put(key[4], val, branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[18]
  EXPECT_EQ(1, worker.ListBranch(key[4]).size());
  EXPECT_FALSE(worker.IsLatest(key[4], ver[17]));
  EXPECT_EQ(ver[18], worker.GetBranchHead(key[4], branch[0]));

  ec = worker.GetForType(UType::kMap, key[4], branch[0], &ucell);
  EXPECT_EQ(ErrorCode::kOK, ec);
  const SMap map2(ucell.dataHash());
  EXPECT_EQ(3, map2.numElements());
  EXPECT_EQ(val_str[7].slice(), map2.Get(val_str[2].slice()));

  // remove an entry in the map
  val.base = map2.hash();
  val.keys.clear();
  val.keys.emplace_back(val_str[1].slice());
  val.vals.clear();

  ec = worker.Put(key[4], val, branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[19]

  ec = worker.GetForType(UType::kMap, key[4], branch[0], &ucell);
  EXPECT_EQ(ErrorCode::kOK, ec);
  const SMap map3(ucell.dataHash());
  EXPECT_EQ(2, map3.numElements());
  size_t n_diffs = 0;
  for (auto itr_diff = UMap::DuallyDiff(map1, map3);
       !itr_diff->end(); itr_diff->next(), ++n_diffs) {
    DLOG(INFO) << "diff at key: " << itr_diff->key();
  }
  EXPECT_EQ(2, n_diffs);
}
