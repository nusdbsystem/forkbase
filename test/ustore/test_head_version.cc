// Copyright (c) 2017 The Ustore Authors.

#include <stdio.h>

#include "gtest/gtest.h"

#include "hash/hash.h"
#include "spec/slice.h"
#include "worker/head_version.h"

using namespace ustore;

const Slice key[] = {Slice("KeyZero"), Slice("KeyOne")};

const Slice branch[] = {
  Slice("BranchMaster"), Slice("BranchFirst"), Slice("BranchSecond"),
  Slice("BranchThird")
};

const Hash ver[] = {
  Hash(reinterpret_cast<const byte_t*>("v0-fake-20-byte-hash")),
  Hash(reinterpret_cast<const byte_t*>("v1-fake-20-byte-hash")),
  Hash(reinterpret_cast<const byte_t*>("v2-fake-20-byte-hash")),
  Hash(reinterpret_cast<const byte_t*>("v3-fake-20-byte-hash")),
  Hash(reinterpret_cast<const byte_t*>("v4-fake-20-byte-hash")),
  Hash(reinterpret_cast<const byte_t*>("v5-fake-20-byte-hash")),
  Hash(reinterpret_cast<const byte_t*>("v6-fake-20-byte-hash")),
  Hash(reinterpret_cast<const byte_t*>("v7-fake-20-byte-hash"))
};

TEST(Worker, Init_LOG) {
  constexpr char test_head_version_log[] = "test_head_version.log";
  DLOG(INFO) << "Creating Empty Head Version";
  HeadVersion* new_head_ver = new HeadVersion(test_head_version_log);

  EXPECT_FALSE(new_head_ver->Exists(key[0], branch[0]));
  EXPECT_EQ(0, new_head_ver->ListBranch(key[0]).size());

  new_head_ver->PutBranch(key[0], branch[0], ver[0]);
  new_head_ver->PutBranch(key[0], branch[1], ver[1]);
  new_head_ver->PutBranch(key[1], branch[2], ver[2]);
  EXPECT_TRUE(new_head_ver->Exists(key[1], branch[2]));
  EXPECT_EQ(2, new_head_ver->ListBranch(key[0]).size());
  EXPECT_TRUE(new_head_ver->IsBranchHead(key[1], branch[2], ver[2]));

  delete new_head_ver;
  DLOG(INFO) << "Destructing Empty Head Version";

  DLOG(INFO) << "Creating Non-Empty Head Version from Log";
  HeadVersion* new_head_ver1 = new HeadVersion(test_head_version_log);
  EXPECT_TRUE(new_head_ver1->Exists(key[1], branch[2]));
  EXPECT_EQ(2, new_head_ver1->ListBranch(key[0]).size());
  EXPECT_EQ(1, new_head_ver1->ListBranch(key[1]).size());
  EXPECT_TRUE(new_head_ver1->IsBranchHead(key[1], branch[2], ver[2]));

  delete new_head_ver1;
  DLOG(INFO) << "Destructing Non-Empty Head Version";

  std::remove(test_head_version_log);
}

HeadVersion head_ver;

TEST(Worker, HeadVersion_PutBranch) {
  EXPECT_FALSE(head_ver.Exists(key[0], branch[0]));
  EXPECT_EQ(0, head_ver.ListBranch(key[0]).size());

  head_ver.PutBranch(key[0], branch[0], ver[0]);
  EXPECT_TRUE(head_ver.Exists(key[0], branch[0]));
  EXPECT_EQ(1, head_ver.ListBranch(key[0]).size());
  EXPECT_TRUE(head_ver.IsBranchHead(key[0], branch[0], ver[0]));

  head_ver.PutBranch(key[0], branch[0], ver[1]);
  EXPECT_FALSE(head_ver.IsBranchHead(key[0], branch[0], ver[0]));
  EXPECT_TRUE(head_ver.IsBranchHead(key[0], branch[0], ver[1]));
  EXPECT_EQ(1, head_ver.ListBranch(key[0]).size());

  head_ver.PutBranch(key[0], branch[1], ver[2]);
  EXPECT_EQ(2, head_ver.ListBranch(key[0]).size());
  EXPECT_TRUE(head_ver.Exists(key[0], branch[1]));

  head_ver.PutBranch(key[0], branch[2], ver[3]);
  EXPECT_TRUE(head_ver.IsBranchHead(key[0], branch[2], ver[3]));
}

TEST(Worker, HeadVersion_GetBranch) {
  EXPECT_EQ(3, head_ver.ListBranch(key[0]).size());
  EXPECT_EQ(ver[1], head_ver.GetBranch(key[0], branch[0]));
  EXPECT_EQ(ver[2], head_ver.GetBranch(key[0], branch[1]));
  EXPECT_EQ(ver[3], head_ver.GetBranch(key[0], branch[2]));
}

TEST(Worker, HeadVersion_PutLatest) {
  head_ver.PutLatest(key[1], Hash::kNull, Hash::kNull, ver[4]);
  EXPECT_TRUE(head_ver.IsLatest(key[1], ver[4]));

  head_ver.PutLatest(key[1], ver[4], Hash::kNull, ver[5]);
  EXPECT_FALSE(head_ver.IsLatest(key[1], ver[4]));
  EXPECT_TRUE(head_ver.IsLatest(key[1], ver[5]));

  head_ver.PutLatest(key[1], Hash::kNull, Hash::kNull, ver[6]);
  head_ver.PutLatest(key[1], ver[5], ver[6], ver[7]);
  EXPECT_FALSE(head_ver.IsLatest(key[1], ver[5]));
  EXPECT_FALSE(head_ver.IsLatest(key[1], ver[6]));
  EXPECT_TRUE(head_ver.IsLatest(key[1], ver[7]));
}

TEST(Worker, HeadVersion_GetLatest) {
  EXPECT_EQ(0, head_ver.GetLatest(key[0]).size());
  EXPECT_EQ(1, head_ver.GetLatest(key[1]).size());
}

TEST(Worker, HeadVersion_RenameBranch) {
  head_ver.RenameBranch(key[0], branch[1], branch[3]);
  EXPECT_FALSE(head_ver.Exists(key[0], branch[1]));
  EXPECT_TRUE(head_ver.Exists(key[0], branch[3]));
  EXPECT_EQ(ver[2], head_ver.GetBranch(key[0], branch[3]));
}

TEST(Worker, HeadVersion_RemoveBranch) {
  EXPECT_EQ(3, head_ver.ListBranch(key[0]).size());

  head_ver.RemoveBranch(key[0], branch[2]);
  EXPECT_FALSE(head_ver.Exists(key[0], branch[2]));
  EXPECT_EQ(2, head_ver.ListBranch(key[0]).size());
}
