// Copyright (c) 2017 The Ustore Authors.

#include <forward_list>
#include <utility>
#include <vector>
#include "gtest/gtest.h"
#include "types/server/factory.h"
#include "types/server/sstring.h"
#include "utils/utils.h"
#include "worker/worker_ext.h"

using namespace ustore;

using SliceFwdList = std::forward_list<Slice>;
using ValueVec = std::vector<Slice>;
using HashVec = std::vector<Hash>;

const Slice key[] = {
  Slice("KeyZero"), Slice("KeyOne"), Slice("KeyTwo"), Slice("KeyThree"),
  Slice("KeyFour"), Slice("KeyFive")
};

const Slice branch[] = {
  Slice("BranchMaster"), Slice("BranchFirst"), Slice("BranchSecond"),
  Slice("BranchThird"), Slice("BranchFourth")
};

const ValueVec vals {
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

HashVec ver;
ErrorCode ec{ErrorCode::kUnknownOp};
std::vector<std::string> branches;
bool exist;
Hash head;
bool is_head;
std::vector<Hash> latest;
bool is_latest;

const WorkerID worker_id{2017};

ustore::WorkerExt& worker() {
  static ustore::WorkerExt* worker = new ustore::WorkerExt(worker_id, false);
  return *worker;
}

TEST(Worker, Meta) {
  EXPECT_EQ(worker_id, worker().id());
}

TEST(Worker, NamedBranch_GetPutString) {
  Hash version;

  worker().ListBranches(key[0], &branches);
  EXPECT_EQ(size_t(0), branches.size());
  worker().Exists(key[0], &exist);
  EXPECT_FALSE(exist);

  Value val0{UType::kString, {}, 0, 0, {vals[0]}, {}};
  ec = worker().Put(key[0], val0, branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[0]
  worker().Exists(key[0], &exist);
  EXPECT_TRUE(exist);
  worker().ListBranches(key[0], &branches);
  EXPECT_EQ(size_t(1), branches.size());
  worker().GetBranchHead(key[0], branch[0], &head);
  EXPECT_EQ(ver[0], head);
  worker().IsLatestVersion(key[0], ver[0], &is_latest);
  EXPECT_TRUE(is_latest);

  Value val1{UType::kString, {}, 0, 0, {vals[1]}, {}};
  ec = worker().Put(key[0], val1, branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[1]
  worker().IsBranchHead(key[0], branch[0], ver[0], &is_head);
  EXPECT_FALSE(is_head);
  worker().IsLatestVersion(key[0], ver[1], &is_latest);
  EXPECT_TRUE(is_latest);

  Value val2{UType::kString, {}, 0, 0, {vals[2]}, {}};
  ec = worker().Put(key[0], val2, branch[1], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[2]
  worker().ListBranches(key[0], &branches);
  EXPECT_EQ(size_t(2), branches.size());

  UCell value;

  ec = worker().Get(key[0], ver[0], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(vals[0], SString(value).slice());

  ec = worker().Get(key[0], branch[0], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(vals[1], SString(value).slice());

  ec = worker().Get(key[0], branch[1], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(vals[2], SString(value).slice());
  worker().GetBranchHead(key[0], branch[1], &head);
  EXPECT_EQ(ver[2], head);
}

TEST(Worker, NamedBranch_Branch) {
  ec = worker().Branch(key[0], branch[0], branch[2]);
  EXPECT_EQ(ErrorCode::kOK, ec);
  worker().ListBranches(key[0], &branches);
  EXPECT_EQ(size_t(3), branches.size());
  EXPECT_EQ(worker().GetBranchHead(key[0], branch[0]),
            worker().GetBranchHead(key[0], branch[2]));
  worker().GetLatestVersions(key[0], &latest);
  EXPECT_EQ(size_t(2), latest.size());
}

TEST(Worker, NamedBranch_Rename) {
  worker().GetBranchHead(key[0], branch[0], &head);
  EXPECT_EQ(ver[1], head);
  ec = worker().Rename(key[0], branch[0], branch[3]);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(head, worker().GetBranchHead(key[0], branch[3]));
  worker().ListBranches(key[0], &branches);
  EXPECT_EQ(size_t(3), branches.size());
  worker().Exists(key[0], branch[0], &exist);
  EXPECT_FALSE(exist);
  worker().Exists(key[0], branch[3], &exist);
  EXPECT_TRUE(exist);
  worker().GetLatestVersions(key[0], &latest);
  EXPECT_EQ(size_t(2), latest.size());
}

TEST(Worker, NamedBranch_Merge) {
  Hash version;

  Value val3{UType::kString, {}, 0, 0, {vals[3]}, {}};
  worker().Merge(key[0], val3, branch[3], branch[1], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[3]
  worker().IsBranchHead(key[0], branch[3], ver[3], &is_head);
  EXPECT_TRUE(is_head);
  worker().IsBranchHead(key[0], branch[1], ver[3], &is_head);
  EXPECT_FALSE(is_head);
  worker().ListBranches(key[0], &branches);
  EXPECT_EQ(size_t(3), branches.size());
  worker().GetLatestVersions(key[0], &latest);
  EXPECT_EQ(size_t(1), latest.size());

  Value val4{UType::kString, {}, 0, 0, {vals[4]}, {}};
  worker().Merge(key[0], val4, branch[1], ver[1], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[4]
  worker().GetLatestVersions(key[0], &latest);
  EXPECT_EQ(size_t(2), latest.size());

  UCell value;

  ec = worker().Get(key[0], branch[3], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(vals[3], SString(value).slice());

  ec = worker().Get(key[0], branch[1], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(vals[4], SString(value).slice());
}

TEST(Worker, UnnamedBranch_GetPutBlob) {
  ChunkableTypeFactory factory;

  Hash version;

  Value val5{UType::kBlob, {}, 0, 0, {vals[5]}, {}};
  worker().Put(key[1], val5, Hash::kNull, &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[5]
  worker().GetLatestVersions(key[1], &latest);
  EXPECT_EQ(size_t(1), latest.size());

  Value val6{UType::kBlob, {}, 0, 0, {vals[6]}, {}};
  worker().Put(key[1], val6, ver[5], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[6]
  worker().GetLatestVersions(key[1], &latest);
  EXPECT_EQ(size_t(1), latest.size());
  worker().IsLatestVersion(key[1], ver[5], &is_latest);
  EXPECT_FALSE(is_latest);
  worker().IsLatestVersion(key[1], ver[6], &is_latest);
  EXPECT_TRUE(is_latest);

  Value val7{UType::kBlob, {}, 0, 0, {vals[7]}, {}};
  worker().Put(key[1], val7, ver[5], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[7]
  worker().GetLatestVersions(key[1], &latest);
  EXPECT_EQ(size_t(2), latest.size());

  Value val8{UType::kBlob, {}, 0, 0, {vals[8]}, {}};
  worker().Put(key[1], val8, ver[7], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[8]
  worker().GetLatestVersions(key[1], &latest);
  EXPECT_EQ(size_t(2), latest.size());

  UCell value;
  SBlob blob = factory.Create<SBlob>(Slice());
  std::unique_ptr<byte_t[]> buf;

  worker().Get(key[1], ver[5], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  blob = factory.Load<SBlob>(value.dataHash());
  buf.reset(new byte_t[blob.size()]);
  blob.Read(0, blob.size(), buf.get());
  EXPECT_EQ(vals[5], Slice(buf.get(), blob.size()));

  worker().Get(key[1], ver[6], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  blob = factory.Load<SBlob>(value.dataHash());
  buf.reset(new byte_t[blob.size()]);
  blob.Read(0, blob.size(), buf.get());
  EXPECT_EQ(vals[6], Slice(buf.get(), blob.size()));

  worker().Get(key[1], ver[7], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  blob = factory.Load<SBlob>(value.dataHash());
  buf.reset(new byte_t[blob.size()]);
  blob.Read(0, blob.size(), buf.get());
  EXPECT_EQ(vals[7], Slice(buf.get(), blob.size()));

  worker().Get(key[1], ver[8], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  blob = factory.Load<SBlob>(value.dataHash());
  buf.reset(new byte_t[blob.size()]);
  blob.Read(0, blob.size(), buf.get());
  EXPECT_EQ(vals[8], Slice(buf.get(), blob.size()));
}

TEST(Worker, UnnamedBranch_Merge) {
  ChunkableTypeFactory factory;

  Hash version;

  Value val9{UType::kBlob, {}, 0, 0, {vals[9]}, {}};
  worker().Merge(key[1], val9, ver[6], ver[7], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[9]
  worker().GetLatestVersions(key[1], &latest);
  EXPECT_EQ(size_t(2), latest.size());

  Value val10{UType::kBlob, {}, 0, 0, {vals[10]}, {}};
  worker().Merge(key[1], val10, ver[8], ver[9], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[10]
  worker().GetLatestVersions(key[1], &latest);
  EXPECT_EQ(size_t(1), latest.size());

  UCell value;
  SBlob blob = factory.Create<SBlob>(Slice());
  std::unique_ptr<byte_t[]> buf;

  worker().Get(key[1], ver[9], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  blob = factory.Load<SBlob>(value.dataHash());
  buf.reset(new byte_t[blob.size()]);
  blob.Read(0, blob.size(), buf.get());
  EXPECT_EQ(vals[9], Slice(buf.get(), blob.size()));

  worker().Get(key[1], ver[10], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  blob = factory.Load<SBlob>(value.dataHash());
  buf.reset(new byte_t[blob.size()]);
  blob.Read(0, blob.size(), buf.get());
  EXPECT_EQ(vals[10], Slice(buf.get(), blob.size()));
}

TEST(Worker, NamedBranch_GetPutList_Value) {
  ChunkableTypeFactory factory;

  Hash version;
  UCell ucell;

  Value val;
  val.type = UType::kList;
  EXPECT_EQ(size_t(0), val.vals.size());

  worker().ListBranches(key[3], &branches);
  EXPECT_EQ(size_t(0), branches.size());

  // construct a new list
  val.base = {};
  val.vals.emplace_back(vals[0]);
  val.vals.emplace_back(vals[1]);
  val.vals.emplace_back(vals[2]);

  ec = worker().Put(key[3], val, branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[11]
  worker().ListBranches(key[3], &branches);
  EXPECT_EQ(size_t(1), branches.size());
  worker().GetBranchHead(key[3], branch[0], &head);
  EXPECT_EQ(ver[11], head);
  worker().IsLatestVersion(key[3], ver[11], &is_latest);
  EXPECT_TRUE(is_latest);

  ec = worker().GetForType(UType::kList, key[3], branch[0], &ucell);
  EXPECT_EQ(ErrorCode::kOK, ec);
  const SList list1 = factory.Load<SList>(ucell.dataHash());
  auto itr_list1 = list1.Scan();
  EXPECT_EQ(vals[0], itr_list1.value());
  EXPECT_TRUE(itr_list1.next());
  EXPECT_EQ(vals[1], itr_list1.value());
  EXPECT_TRUE(itr_list1.next());
  EXPECT_FALSE(itr_list1.next());
  EXPECT_EQ(vals[2], list1.Get(2));

  // update existing list by replacing one element
  val.base = list1.hash();
  val.pos = 1;
  val.dels = 1;
  val.vals.clear();
  val.vals.emplace_back(vals[3]);

  ec = worker().Put(key[3], val, branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[12]
  worker().ListBranches(key[3], &branches);
  EXPECT_EQ(size_t(1), branches.size());
  worker().IsLatestVersion(key[3], ver[11], &is_latest);
  EXPECT_FALSE(is_latest);
  worker().GetBranchHead(key[3], branch[0], &head);
  EXPECT_EQ(ver[12], head);

  ec = worker().GetForType(UType::kList, key[3], branch[0], &ucell);
  EXPECT_EQ(ErrorCode::kOK, ec);
  const SList list2 = factory.Load<SList>(ucell.dataHash());
  EXPECT_EQ(size_t(3), list2.numElements());
  EXPECT_EQ(vals[0], list2.Get(0));
  EXPECT_EQ(vals[3], list2.Get(1));
  EXPECT_EQ(vals[2], list2.Get(2));

  // update existing list by inserting new elements
  val.base = list2.hash();
  val.pos = 1;
  val.dels = 0;
  val.vals.clear();
  val.vals.emplace_back(vals[4]);
  val.vals.emplace_back(vals[5]);

  ec = worker().Put(key[3], val, branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[13]

  // diff on two lists
  ec = worker().GetForType(UType::kList, key[3], branch[0], &ucell);
  EXPECT_EQ(ErrorCode::kOK, ec);
  const SList list3 = factory.Load<SList>(ucell.dataHash());
  EXPECT_EQ(size_t(5), list3.numElements());
  auto itr_diff = UList::DuallyDiff(list1, list3);
  EXPECT_EQ(size_t(1), itr_diff.index());
  EXPECT_EQ(vals[1], itr_diff.lhs_value());
  EXPECT_EQ(vals[4], itr_diff.rhs_value());
  EXPECT_TRUE(itr_diff.next());
  EXPECT_EQ(size_t(2), itr_diff.index());
  EXPECT_EQ(vals[2], itr_diff.lhs_value());
  EXPECT_EQ(vals[5], itr_diff.rhs_value());
  EXPECT_TRUE(itr_diff.next());
  EXPECT_EQ(size_t(3), itr_diff.index());
  EXPECT_TRUE(itr_diff.lhs_value().empty());
  EXPECT_EQ(vals[3], itr_diff.rhs_value());
  EXPECT_TRUE(itr_diff.next());
  EXPECT_EQ(size_t(4), itr_diff.index());
  EXPECT_TRUE(itr_diff.lhs_value().empty());
  EXPECT_EQ(vals[2], itr_diff.rhs_value());
  EXPECT_FALSE(itr_diff.next());
}

TEST(Worker, NamedBranch_GetPutMap_Value) {
  ChunkableTypeFactory factory;

  Hash version;
  UCell ucell;

  Value val = {};
  val.type = UType::kMap;
  EXPECT_EQ(size_t(0), val.keys.size());
  EXPECT_EQ(size_t(0), val.vals.size());

  worker().ListBranches(key[4], &branches);
  EXPECT_EQ(size_t(0), branches.size());

  // construct a new map
  val.base = {};
  val.keys.clear();
  val.keys.emplace_back(vals[0]);
  val.keys.emplace_back(vals[1]);
  val.vals.clear();
  val.vals.emplace_back(vals[5]);
  val.vals.emplace_back(vals[6]);

  ec = worker().Put(key[4], val, branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[14]
  worker().ListBranches(key[4], &branches);
  EXPECT_EQ(size_t(1), branches.size());
  worker().GetBranchHead(key[4], branch[0], &head);
  EXPECT_EQ(ver[14], head);
  worker().IsLatestVersion(key[4], ver[14], &is_latest);
  EXPECT_TRUE(is_latest);

  ec = worker().GetForType(UType::kMap, key[4], branch[0], &ucell);
  EXPECT_EQ(ErrorCode::kOK, ec);
  const SMap map1 = factory.Load<SMap>(ucell.dataHash());
  EXPECT_EQ(vals[5], map1.Get(vals[0]));
  EXPECT_EQ(vals[6], map1.Get(vals[1]));

  // update an entry in the map
  val.base = map1.hash();
  val.keys.clear();
  val.keys.emplace_back(vals[2]);
  val.vals.clear();
  val.vals.emplace_back(vals[7]);

  ec = worker().Put(key[4], val, branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[15]
  worker().ListBranches(key[4], &branches);
  EXPECT_EQ(size_t(1), branches.size());
  worker().IsLatestVersion(key[4], ver[14], &is_latest);
  EXPECT_FALSE(is_latest);
  worker().GetBranchHead(key[4], branch[0], &head);
  EXPECT_EQ(ver[15], head);

  ec = worker().GetForType(UType::kMap, key[4], branch[0], &ucell);
  EXPECT_EQ(ErrorCode::kOK, ec);
  const SMap map2 = factory.Load<SMap>(ucell.dataHash());
  EXPECT_EQ(size_t(3), map2.numElements());
  EXPECT_EQ(vals[7], map2.Get(vals[2]));

  // remove an entry in the map
  val.base = map2.hash();
  val.keys.clear();
  val.keys.emplace_back(vals[1]);
  val.vals.clear();
  val.dels = 1;

  ec = worker().Put(key[4], val, branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version));  // ver[16]

  ec = worker().GetForType(UType::kMap, key[4], branch[0], &ucell);
  EXPECT_EQ(ErrorCode::kOK, ec);
  const SMap map3 = factory.Load<SMap>(ucell.dataHash());
  EXPECT_EQ(size_t(2), map3.numElements());
  size_t n_diffs = 0;
  for (auto itr_diff = UMap::DuallyDiff(map1, map3);
       !itr_diff.end(); itr_diff.next(), ++n_diffs) {
    DLOG(INFO) << "diff at key: " << itr_diff.key();
  }
  EXPECT_EQ(size_t(2), n_diffs);
}

TEST(Worker, DeleteBranch) {
  ec = worker().Delete(key[0], branch[1]);
  EXPECT_EQ(ErrorCode::kOK, ec);
  worker().Exists(key[0], branch[1], &exist);
  EXPECT_FALSE(exist);
}
