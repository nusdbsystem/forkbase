// Copyright (c) 2017 The Ustore Authors.

#include <forward_list>
#include <utility>
#include <vector>
#include "gtest/gtest.h"
#include "spec/slice.h"
#include "spec/value.h"
#include "types/type.h"
#include "worker/worker.h"

using namespace ustore;
using SliceFwdList = std::forward_list<Slice>;
using ValueVec = std::vector<Value>;
using HashVec = std::vector<Hash>;

const Slice key[] = {Slice("KeyZero"), Slice("KeyOne"), Slice("KeyThree")};

const Slice branch[] = {
  Slice("BranchMaster"), Slice("BranchFirst"), Slice("BranchSecond"),
  Slice("BranchThird"), Slice("BranchFourth")
};

const SliceFwdList slices {
  Slice("The quick brown fox jumps over the lazy dog"), //-- 0
  Slice("Edge of tomorrow"), //----------------------------- 1
  Slice("Pig can fly!"), //--------------------------------- 2
  Slice("Have you ever seen the rain?"), //----------------- 3
  Slice("Once upon a time"), //----------------------------- 4
  Slice("Good good study, day day up!"), //----------------- 5
  Slice("I am a hero"), //---------------------------------- 6
  Slice("Mad detector"), //--------------------------------- 7
  Slice("Stay hungry. Stay foolish."), //------------------- 8
  Slice("To be, or not to be, that is the question."), //--- 9
  Slice("What goes around, comes around") //---------------- 10
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
  return Blob(reinterpret_cast<const byte_t*>(s.data()), s.len());
}));

HashVec ver;
ErrorCode ec{ErrorCode::kUnknownOp};

const WorkerID worker_id{14};
Worker worker{worker_id};

TEST(Worker, Meta) {
  EXPECT_EQ(worker_id, worker.id());
}

TEST(Worker, NamedBranch_GetPutString) {
  Hash version;

  EXPECT_EQ(0, worker.ListBranch(key[0]).size());

  ec = worker.Put(key[0], val_str[0], branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version)); // ver[0]
  EXPECT_EQ(1, worker.ListBranch(key[0]).size());
  EXPECT_EQ(ver[0], worker.GetBranchHead(key[0], branch[0]));
  EXPECT_TRUE(worker.IsLatest(key[0], ver[0]));

  ec = worker.Put(key[0], val_str[1], branch[0], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version)); // ver[1]
  EXPECT_FALSE(worker.IsBranchHead(key[0], branch[0], ver[0]));
  EXPECT_TRUE(worker.IsLatest(key[0], ver[1]));

  ec = worker.Put(key[0], val_str[2], branch[1], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version)); // ver[2]
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
  ver.push_back(std::move(version)); // ver[3]
  EXPECT_TRUE(worker.IsBranchHead(key[0], branch[3], ver[3]));
  EXPECT_FALSE(worker.IsBranchHead(key[0], branch[1], ver[3]));
  EXPECT_EQ(3, worker.ListBranch(key[0]).size());
  EXPECT_EQ(1, worker.GetLatestVersions(key[0]).size());

  worker.Merge(key[0], val_str[4], branch[1], ver[1], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version)); // ver[4]
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
  ver.push_back(std::move(version)); // ver[5]
  EXPECT_EQ(1, worker.GetLatestVersions(key[1]).size());

  worker.Put(key[1], val_blob[6], ver[5], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version)); // ver[6]
  EXPECT_EQ(1, worker.GetLatestVersions(key[1]).size());
  EXPECT_FALSE(worker.IsLatest(key[1], ver[5]));
  EXPECT_TRUE(worker.IsLatest(key[1], ver[6]));

  worker.Put(key[1], val_blob[7], ver[5], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version)); // ver[7]
  EXPECT_EQ(2, worker.GetLatestVersions(key[1]).size());

  worker.Put(key[1], val_blob[8], ver[7], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version)); // ver[8]
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
  ver.push_back(std::move(version)); // ver[9]
  EXPECT_EQ(2, worker.GetLatestVersions(key[1]).size());

  worker.Merge(key[1], val_blob[10], ver[8], ver[9], &version);
  EXPECT_EQ(ErrorCode::kOK, ec);
  ver.push_back(std::move(version)); // ver[10]
  EXPECT_EQ(1, worker.GetLatestVersions(key[1]).size());

  Value value;

  worker.Get(key[1], ver[9], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(val_blob[9], value);

  worker.Get(key[1], ver[10], &value);
  EXPECT_EQ(ErrorCode::kOK, ec);
  EXPECT_EQ(val_blob[10], value);
}
