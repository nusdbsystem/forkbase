// Copyright (c) 2017 The Ustore Authors.

#include <stdio.h>

#include "gtest/gtest.h"

#include "hash/hash.h"
#include "spec/slice.h"
#include "worker/rocksdb_head_version.h"

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

RocksDBHeadVersion rocksdb_head_ver;
constexpr char head_version_db[] = "head_version.rocksdb";

TEST(RocksDBHeadVersion, Load) {
  RocksDBHeadVersion::DeleteDB(head_version_db);
  EXPECT_TRUE(rocksdb_head_ver.LoadBranchVersion(head_version_db));
}

TEST(RocksDBHeadVersion, PutBranch) {
  EXPECT_FALSE(rocksdb_head_ver.Exists(key[0], branch[0]));
  EXPECT_EQ(size_t(0), rocksdb_head_ver.ListBranch(key[0]).size());

  rocksdb_head_ver.PutBranch(key[0], branch[0], ver[0]);
  EXPECT_TRUE(rocksdb_head_ver.Exists(key[0], branch[0]));
  EXPECT_EQ(size_t(1), rocksdb_head_ver.ListBranch(key[0]).size());
  EXPECT_TRUE(rocksdb_head_ver.IsBranchHead(key[0], branch[0], ver[0]));

  rocksdb_head_ver.PutBranch(key[0], branch[0], ver[1]);
  EXPECT_FALSE(rocksdb_head_ver.IsBranchHead(key[0], branch[0], ver[0]));
  EXPECT_TRUE(rocksdb_head_ver.IsBranchHead(key[0], branch[0], ver[1]));
  EXPECT_EQ(size_t(1), rocksdb_head_ver.ListBranch(key[0]).size());

  rocksdb_head_ver.PutBranch(key[0], branch[1], ver[2]);
  EXPECT_EQ(size_t(2), rocksdb_head_ver.ListBranch(key[0]).size());
  EXPECT_TRUE(rocksdb_head_ver.Exists(key[0], branch[1]));

  rocksdb_head_ver.PutBranch(key[0], branch[2], ver[3]);
  EXPECT_TRUE(rocksdb_head_ver.IsBranchHead(key[0], branch[2], ver[3]));

  ASSERT_TRUE(rocksdb_head_ver.DumpBranchVersion(head_version_db));
}

TEST(RocksDBHeadVersion, GetBranch) {
  Hash version;
  EXPECT_FALSE(rocksdb_head_ver.LoadBranchVersion(head_version_db));
  EXPECT_EQ(size_t(3), rocksdb_head_ver.ListBranch(key[0]).size());
  EXPECT_TRUE(rocksdb_head_ver.GetBranch(key[0], branch[0], &version));
  EXPECT_EQ(ver[1], version);
  EXPECT_TRUE(rocksdb_head_ver.GetBranch(key[0], branch[1], &version));
  EXPECT_EQ(ver[2], version);
  EXPECT_TRUE(rocksdb_head_ver.GetBranch(key[0], branch[2], &version));
  EXPECT_EQ(ver[3], version);
}

TEST(RocksDBHeadVersion, PutLatest) {
  rocksdb_head_ver.PutLatest(key[1], Hash::kNull, Hash::kNull, ver[4]);
  EXPECT_TRUE(rocksdb_head_ver.IsLatest(key[1], ver[4]));

  rocksdb_head_ver.PutLatest(key[1], ver[4], Hash::kNull, ver[5]);
  EXPECT_FALSE(rocksdb_head_ver.IsLatest(key[1], ver[4]));
  EXPECT_TRUE(rocksdb_head_ver.IsLatest(key[1], ver[5]));

  rocksdb_head_ver.PutLatest(key[1], Hash::kNull, Hash::kNull, ver[6]);
  rocksdb_head_ver.PutLatest(key[1], ver[5], ver[6], ver[7]);
  EXPECT_FALSE(rocksdb_head_ver.IsLatest(key[1], ver[5]));
  EXPECT_FALSE(rocksdb_head_ver.IsLatest(key[1], ver[6]));
  EXPECT_TRUE(rocksdb_head_ver.IsLatest(key[1], ver[7]));
}

TEST(RocksDBHeadVersion, GetLatest) {
  EXPECT_EQ(size_t(0), rocksdb_head_ver.GetLatest(key[0]).size());
  EXPECT_EQ(size_t(1), rocksdb_head_ver.GetLatest(key[1]).size());
}

TEST(RocksDBHeadVersion, RenameBranch) {
  Hash version;
  rocksdb_head_ver.RenameBranch(key[0], branch[1], branch[3]);
  EXPECT_FALSE(rocksdb_head_ver.Exists(key[0], branch[1]));
  EXPECT_TRUE(rocksdb_head_ver.Exists(key[0], branch[3]));
  EXPECT_TRUE(rocksdb_head_ver.GetBranch(key[0], branch[3], &version));
  EXPECT_EQ(ver[2], version);
}

TEST(RocksDBHeadVersion, RemoveBranch) {
  EXPECT_EQ(size_t(3), rocksdb_head_ver.ListBranch(key[0]).size());

  rocksdb_head_ver.RemoveBranch(key[0], branch[2]);
  EXPECT_FALSE(rocksdb_head_ver.Exists(key[0], branch[2]));
  EXPECT_EQ(size_t(2), rocksdb_head_ver.ListBranch(key[0]).size());
}

TEST(RocksDBHeadVersion, Dump) {
  EXPECT_TRUE(rocksdb_head_ver.DumpBranchVersion(head_version_db));
  rocksdb_head_ver.CloseDB();
}
