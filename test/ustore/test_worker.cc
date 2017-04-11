// Copyright (c) 2017 The Ustore Authors.

#include <vector>
#include <unordered_set>
#include "gtest/gtest.h"
#include "spec/slice.h"
#include "spec/value.h"
#include "types/type.h"
#include "worker/worker.h"

using namespace ustore;

Worker worker {27};

// Note: Forcing users to be responsible for the durability of the data
//       could be an issue.

const Slice key1("KeyOne");
const Slice key2("KeyTwo");

const Slice branch1("BranchFirst");
const Slice branch2("BranchSecond");
const Slice branch3("BranchThird");
const Slice branch4("BranchFourth");

const Slice slice1("The quick brown fox jumps over the lazy dog");
const Slice slice2("Edge of tomorrow");
const Slice slice3("Pig can fly!");
const Slice slice4("Have you ever seen the rain?");
const Slice slice5("Once upon a time");
const Slice slice6("Good good study, day day up!");

const Blob blob1(reinterpret_cast<const byte_t*>(slice1.data()), slice1.len());
const Blob blob2(reinterpret_cast<const byte_t*>(slice2.data()), slice2.len());
const Blob blob3(reinterpret_cast<const byte_t*>(slice3.data()), slice3.len());
const Blob blob4(reinterpret_cast<const byte_t*>(slice4.data()), slice4.len());

TEST(Worker, PutString) {
  EXPECT_EQ(0, worker.ListBranch(key1).size());

  const byte_t ver0_raw[] = "000";
  const Hash ver0(ver0_raw);

  Hash ver1;
  EXPECT_EQ(ErrorCode::kOK, worker.Put(key1, Value(slice1), branch1, ver0, &ver1));
  EXPECT_EQ(ver1, worker.GetBranchHead(key1, branch1));
  EXPECT_EQ(1, worker.ListBranch(key1).size());
  EXPECT_TRUE(worker.IsBranchHead(key1, branch1, ver1));

  Hash ver2;
  EXPECT_EQ(ErrorCode::kOK, worker.Put(key1, Value(slice2), branch1, ver1, &ver2));
  EXPECT_FALSE(worker.IsBranchHead(key1, branch1, ver1));
  EXPECT_EQ(ver2, worker.GetBranchHead(key1, branch1));

  Hash ver3;
  EXPECT_EQ(ErrorCode::kOK, worker.Put(key1, Value(slice3), ver2, &ver3));
  EXPECT_FALSE(worker.IsLatest(key1, worker.GetBranchHead(key1, branch1)));
  EXPECT_EQ(1, worker.GetLatestVersions(key1).size());

  Hash ver4;
  EXPECT_EQ(ErrorCode::kOK, worker.Put(key1, Value(slice4), ver2, &ver4));
  EXPECT_EQ(2, worker.GetLatestVersions(key1).size());

  Hash ver5;
  EXPECT_EQ(ErrorCode::kOK, worker.Put(key1, Value(slice2), branch2, ver0, &ver5));
  EXPECT_EQ(ErrorCode::kOK, worker.Put(key1, Value(slice1), branch2, ver5, &ver5));
  EXPECT_EQ(ver5, worker.GetBranchHead(key1, branch2));
  EXPECT_EQ(2, worker.ListBranch(key1).size());
  EXPECT_EQ(3, worker.GetLatestVersions(key1).size());
}

TEST(Worker, GetString) {
  Value val_b1;
  EXPECT_EQ(ErrorCode::kOK, worker.Get(key1, branch1, &val_b1));
  EXPECT_EQ(UType::kString, val_b1.type());
  EXPECT_EQ(slice2, val_b1.slice());

  Value val_b2;
  const auto head_key1_b2 = worker.GetBranchHead(key1, branch2);
  EXPECT_EQ(ErrorCode::kOK, worker.Get(key1, head_key1_b2, &val_b2));
  EXPECT_EQ(slice1, val_b2.slice());

  for (auto && ver : worker.GetLatestVersions(key1)) {
    Value val;
    EXPECT_EQ(ErrorCode::kOK, worker.Get(key1, Worker::kNullBranch, ver, &val));
  }
}

TEST(Worker, PutBlob) {
  EXPECT_EQ(0, worker.ListBranch(key2).size());

  const byte_t ver0_raw[] = "000";
  const Hash ver0(ver0_raw);

  Hash ver1;
  EXPECT_EQ(ErrorCode::kOK, worker.Put(key2, Value(blob1), branch1, ver0, &ver1));
  EXPECT_EQ(ver1, worker.GetBranchHead(key2, branch1));
  EXPECT_EQ(1, worker.ListBranch(key2).size());
  EXPECT_TRUE(worker.IsBranchHead(key2, branch1, ver1));

  Hash ver2;
  EXPECT_EQ(ErrorCode::kOK, worker.Put(key2, Value(blob2), branch1, ver1, &ver2));
  EXPECT_FALSE(worker.IsBranchHead(key2, branch1, ver1));
  EXPECT_EQ(ver2, worker.GetBranchHead(key2, branch1));

  Hash ver3;
  EXPECT_EQ(ErrorCode::kOK, worker.Put(key2, Value(blob3), ver2, &ver3));
  EXPECT_FALSE(worker.IsLatest(key2, worker.GetBranchHead(key2, branch1)));

  Hash ver4;
  EXPECT_EQ(ErrorCode::kOK, worker.Put(key2, Value(blob4), ver2, &ver4));
  EXPECT_EQ(2, worker.GetLatestVersions(key2).size());

  Hash ver5;
  EXPECT_EQ(ErrorCode::kOK, worker.Put(key2, Value(blob2), branch2, ver0, &ver5));
  EXPECT_EQ(ErrorCode::kOK, worker.Put(key2, Value(blob1), branch2, ver5, &ver5));
  EXPECT_EQ(ver5, worker.GetBranchHead(key2, branch2));
  EXPECT_EQ(2, worker.ListBranch(key2).size());
  EXPECT_EQ(3, worker.GetLatestVersions(key2).size());
}

TEST(Worker, GetBlob) {
  Value val_b1;
  EXPECT_EQ(ErrorCode::kOK, worker.Get(key2, branch1, &val_b1));
  EXPECT_EQ(UType::kBlob, val_b1.type());
  EXPECT_EQ(blob2, val_b1.blob());

  Value val_b2;
  const auto head_key2_b2 = worker.GetBranchHead(key2, branch2);
  EXPECT_EQ(ErrorCode::kOK, worker.Get(key2, head_key2_b2, &val_b2));
  EXPECT_EQ(blob1, val_b2.blob());

  for (auto && ver : worker.GetLatestVersions(key2)) {
    Value val;
    EXPECT_EQ(ErrorCode::kOK, worker.Get(key2, Worker::kNullBranch, ver, &val));
  }
}

TEST(Worker, Branch) {
  EXPECT_EQ(ErrorCode::kOK, worker.Branch(key1, branch1, branch3));
  EXPECT_EQ(3, worker.ListBranch(key1).size());
  EXPECT_EQ(worker.GetBranchHead(key1, branch1), worker.GetBranchHead(key1, branch3));
  EXPECT_EQ(3, worker.GetLatestVersions(key1).size());
}

TEST(Worker, RenameBranch) {
  const Hash head_b1 = worker.GetBranchHead(key1, branch1);
  EXPECT_EQ(ErrorCode::kOK, worker.Move(key1, branch1, branch4));
  EXPECT_EQ(head_b1, worker.GetBranchHead(key1, branch4));
  const auto branches_key1 = worker.ListBranch(key1);
  EXPECT_EQ(3, branches_key1.size());
  EXPECT_EQ(branches_key1.end(), branches_key1.find(branch1));
  EXPECT_NE(branches_key1.end(), branches_key1.find(branch4));
  EXPECT_EQ(3, worker.GetLatestVersions(key1).size());
}

TEST(Worker, Merge) {
  Hash head_b4;
  EXPECT_EQ(ErrorCode::kOK, worker.Merge(key1, Value(slice5), branch4, branch2, &head_b4));
  EXPECT_TRUE(worker.IsBranchHead(key1, branch4, head_b4));
  EXPECT_EQ(3, worker.ListBranch(key1).size());
  EXPECT_EQ(4, worker.GetLatestVersions(key1).size());

  const Hash head_b3 = worker.GetBranchHead(key1, branch3);
  Hash head_b2;
  EXPECT_EQ(ErrorCode::kOK, worker.Merge(key1, Value(slice6), branch2, head_b3, &head_b2));
  EXPECT_TRUE(worker.IsBranchHead(key1, branch2, head_b2));
  EXPECT_EQ(3, worker.ListBranch(key1).size());
  EXPECT_EQ(4, worker.GetLatestVersions(key1).size());

  std::unordered_set<Hash> dangling_ver(worker.GetLatestVersions(key1));
  for (auto && b : worker.ListBranch(key1)) {
    dangling_ver.erase(worker.GetBranchHead(key1, b));
  }
  EXPECT_EQ(2, dangling_ver.size());
  auto dangling_ver_itr = dangling_ver.begin();
  const Hash dv0 = (*dangling_ver_itr).Clone();
  const Hash dv1 = (*(++dangling_ver_itr)).Clone();
  Hash dv_merge;
  EXPECT_EQ(ErrorCode::kOK, worker.Merge(key1, Value(slice6), dv0, dv1, &dv_merge));
  EXPECT_EQ(3, worker.ListBranch(key1).size());
  EXPECT_EQ(3, worker.GetLatestVersions(key1).size());
}
