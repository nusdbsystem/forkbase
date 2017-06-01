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

HeadVersion head_ver;
constexpr char test_head_version_log[] = "test_head_version.log";

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

  ASSERT_TRUE(head_ver.DumpBranchVersion(test_head_version_log));
}

TEST(Worker, HeadVersion_Load) {
  HeadVersion new_head_ver;
  new_head_ver.LoadBranchVersion(test_head_version_log);
  EXPECT_EQ(3, new_head_ver.ListBranch(key[0]).size());
  EXPECT_EQ(ver[1], new_head_ver.GetBranch(key[0], branch[0]));
  EXPECT_EQ(ver[2], new_head_ver.GetBranch(key[0], branch[1]));
  EXPECT_EQ(ver[3], new_head_ver.GetBranch(key[0], branch[2]));
  std::remove(test_head_version_log);
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
