// Copyright (c) 2017 The Ustore Authors.

#include <stdio.h>

#include "gtest/gtest.h"

#include "hash/hash.h"
#include "spec/slice.h"
#include "worker/simple_head_version.h"

using namespace ustore;

const Slice key[] = {Slice("KeyZero"), Slice("KeyOne")};

const Slice branch[] = {
  Slice("BranchMaster"), Slice("BranchFirst"), Slice("BranchSecond"),
  Slice("BranchThird")
};

const Hash ver[] = {
  Hash::ComputeFrom("v0"),
  Hash::ComputeFrom("v1"),
  Hash::ComputeFrom("v2"),
  Hash::ComputeFrom("v3"),
  Hash::ComputeFrom("v4"),
  Hash::ComputeFrom("v5"),
  Hash::ComputeFrom("v6"),
  Hash::ComputeFrom("v7")
};

SimpleHeadVersion head_ver;
constexpr char test_head_version_log[] = "test_head_version.log";

TEST(SimpleHeadVersion, PutBranch) {
  EXPECT_FALSE(head_ver.Exists(key[0], branch[0]));
  EXPECT_EQ(size_t(0), head_ver.ListBranch(key[0]).size());

  head_ver.PutBranch(key[0], branch[0], ver[0]);
  EXPECT_TRUE(head_ver.Exists(key[0], branch[0]));
  EXPECT_EQ(size_t(1), head_ver.ListBranch(key[0]).size());
  EXPECT_TRUE(head_ver.IsBranchHead(key[0], branch[0], ver[0]));

  head_ver.PutBranch(key[0], branch[0], ver[1]);
  EXPECT_FALSE(head_ver.IsBranchHead(key[0], branch[0], ver[0]));
  EXPECT_TRUE(head_ver.IsBranchHead(key[0], branch[0], ver[1]));
  EXPECT_EQ(size_t(1), head_ver.ListBranch(key[0]).size());

  head_ver.PutBranch(key[0], branch[1], ver[2]);
  EXPECT_EQ(size_t(2), head_ver.ListBranch(key[0]).size());
  EXPECT_TRUE(head_ver.Exists(key[0], branch[1]));

  head_ver.PutBranch(key[0], branch[2], ver[3]);
  EXPECT_TRUE(head_ver.IsBranchHead(key[0], branch[2], ver[3]));

  ASSERT_TRUE(head_ver.DumpBranchVersion(test_head_version_log));
}

TEST(SimpleHeadVersion, Load) {
  SimpleHeadVersion new_head_ver;
  Hash version;
  new_head_ver.LoadBranchVersion(test_head_version_log);
  EXPECT_EQ(size_t(3), new_head_ver.ListBranch(key[0]).size());
  EXPECT_TRUE(new_head_ver.GetBranch(key[0], branch[0], &version));
  EXPECT_EQ(ver[1], version);
  EXPECT_TRUE(new_head_ver.GetBranch(key[0], branch[1], &version));
  EXPECT_EQ(ver[2], version);
  EXPECT_TRUE(new_head_ver.GetBranch(key[0], branch[2], &version));
  EXPECT_EQ(ver[3], version);
  std::remove(test_head_version_log);
}

TEST(SimpleHeadVersion, GetBranch) {
  Hash version;
  EXPECT_EQ(size_t(3), head_ver.ListBranch(key[0]).size());
  EXPECT_TRUE(head_ver.GetBranch(key[0], branch[0], &version));
  EXPECT_EQ(ver[1], version);
  EXPECT_TRUE(head_ver.GetBranch(key[0], branch[1], &version));
  EXPECT_EQ(ver[2], version);
  EXPECT_TRUE(head_ver.GetBranch(key[0], branch[2], &version));
  EXPECT_EQ(ver[3], version);
}

TEST(SimpleHeadVersion, PutLatest) {
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

TEST(SimpleHeadVersion, GetLatest) {
  EXPECT_EQ(size_t(0), head_ver.GetLatest(key[0]).size());
  EXPECT_EQ(size_t(1), head_ver.GetLatest(key[1]).size());
}

TEST(SimpleHeadVersion, RenameBranch) {
  Hash version;
  head_ver.RenameBranch(key[0], branch[1], branch[3]);
  EXPECT_FALSE(head_ver.Exists(key[0], branch[1]));
  EXPECT_TRUE(head_ver.Exists(key[0], branch[3]));
  EXPECT_TRUE(head_ver.GetBranch(key[0], branch[3], &version));
  EXPECT_EQ(ver[2], version);
}

TEST(SimpleHeadVersion, RemoveBranch) {
  EXPECT_EQ(size_t(3), head_ver.ListBranch(key[0]).size());

  head_ver.RemoveBranch(key[0], branch[2]);
  EXPECT_FALSE(head_ver.Exists(key[0], branch[2]));
  EXPECT_EQ(size_t(2), head_ver.ListBranch(key[0]).size());
}
